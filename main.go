package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync"

	"github.com/cheggaaa/pb/v3"
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
)

var (
	dsn             string
	testDB          string
	sizeStr         string
	bulkInsertCount int
	rowSizeBytes    int64

	sizeMB  int64
	threads int64
)

func init() {
	flag.StringVar(&dsn, "dsn", "root:secretpassword@tcp(localhost)/mysql", "MariaDB DSN")
	flag.StringVar(&testDB, "test-db", "", "Database to create")
	flag.StringVar(&sizeStr, "size", "1G", "Database size to generate ('3' or '3G' => 3GB, '10M' => 10MB, '1T' => 1TB)")
	flag.Int64Var(&threads, "threads", 1, "Number of threads to use")
	flag.IntVar(&bulkInsertCount, "bulk-count", 4000, "Number of rows to insert per query")
	flag.Int64Var(&rowSizeBytes, "row-size-bytes", 2048, "Row size in bytes")
}

func validateFlags() error {
	if testDB == "" {
		return fmt.Errorf("Please provide a test database name")
	}

	var mul int64 = 1000
	var n int
	var err error
	switch {
	case strings.HasSuffix(sizeStr, "G"):
		mul = 1000
		n, err = strconv.Atoi(strings.TrimSuffix(sizeStr, "G"))
		if err != nil {
			return err
		}
	case strings.HasSuffix(sizeStr, "M"):
		mul = 1
		n, err = strconv.Atoi(strings.TrimSuffix(sizeStr, "M"))
		if err != nil {
			return err
		}
	case strings.HasSuffix(sizeStr, "T"):
		mul = 1000000
		n, err = strconv.Atoi(strings.TrimSuffix(sizeStr, "T"))
		if err != nil {
			return err
		}
	}
	sizeMB = int64(n) * mul
	return nil
}

// DB represents the database connection
type DB struct {
	OriginalDSN string
	Config      *mysql.Config
	TestDB      string

	c *sql.DB
}

// InitDB initializes the db connection, creating the test db if needed.
func InitDB(ctx context.Context, dsn, testDB string) (*DB, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	hdl, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	defer hdl.Close()

	// Check connectivity
	err = hdl.PingContext(ctx)
	if err != nil {
		return nil, err
	}

	// Create test db
	_, err = hdl.ExecContext(ctx, fmt.Sprintf(`CREATE DATABASE %s`, testDB))
	if err != nil {
		return nil, err
	}

	cfg.DBName = testDB
	newdsn := cfg.FormatDSN()
	hdl, err = sql.Open("mysql", newdsn)
	if err != nil {
		return nil, err
	}

	// Check connectivity again.
	err = hdl.PingContext(ctx)
	if err != nil {
		return nil, err
	}

	return &DB{
		OriginalDSN: dsn,
		Config:      cfg,
		TestDB:      testDB,
		c:           hdl,
	}, nil
}

// Close active db handles
func (db *DB) Close() error {
	return db.c.Close()
}

// CreateTable creates a table for population.
func (db *DB) CreateTable(ctx context.Context, name string) error {
	_, err := db.c.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %s (id SERIAL, data MEDIUMTEXT)`, name))
	return err
}

// GenData generates a string of length n
func GenData(n int) string {
	chars := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	r := make([]rune, n)
	for i := range r {
		r[i] = chars[int(b[i])%len(chars)]
	}
	return string(r)
}

func (db *DB) InsertRows(ctx context.Context, stmt *sql.Stmt, rows []any, count int) error {
	// start := time.Now()
	r, err := stmt.ExecContext(ctx, rows...)
	if err != nil {
		return err
	}
	affected, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if affected != int64(count) {
		return fmt.Errorf("Expected to insert %d rows, but inserted %d", count, affected)
	}
	// log.Printf("Insert %d rows: took %s\n", count, time.Since(start))
	return nil
}

func main() {
	flag.Parse()

	err := validateFlags()
	if err != nil {
		log.Fatalf("Bad size: %v", err)
	}

	ctx := context.Background()
	db, err := InitDB(ctx, dsn, testDB)
	if err != nil {
		log.Fatalf("Error initializing database: %v", err)
	}
	defer db.Close()

	var (
		// Random data per row (8 bytes come from the row ID)
		randDataSize = int(rowSizeBytes - 8)

		totalDataSizeBytes float64 = float64(sizeMB * 1000 * 1000)
		// Data per table:
		perThreadBytes float64 = math.Ceil(totalDataSizeBytes / float64(threads))
		// Row count per table:
		numRows int64 = int64(math.Ceil(perThreadBytes / float64(rowSizeBytes)))
		// Total insert queries:
		totalInserts = threads * ((numRows + int64(bulkInsertCount) - 1) / int64(bulkInsertCount))
	)

	log.Printf("Generating %d table(s) with %d rows (%d bytes each) per table\n", threads, numRows, rowSizeBytes)

	rowsCh := make(chan []any, 2*threads)
	go func() {
		defer close(rowsCh)
		for i := int64(0); i < totalInserts; i++ {
			// start := time.Now()
			rows := make([]any, bulkInsertCount)
			for i := range rows {
				rows[i] = GenData(randDataSize)
			}
			rowsCh <- rows
			// datagenDur := time.Since(start)
		}
	}()

	// Init progress bar
	fmt.Printf("total inserts: %d\n", totalInserts)
	// bar := pb.StartNew(int(totalInserts))
	bar := pb.Full.Start(int(totalInserts))

	// Launch threads to create tables.
	wg := sync.WaitGroup{}
	for i := int64(0); i < threads; i++ {
		wg.Add(1)
		go func(tableName string) {
			defer wg.Done()
			err := db.CreateTable(ctx, tableName)
			if err != nil {
				log.Fatalf("Error initializing database: %v", err)
				panic(err)
			}

			// Build prepared query for this thread/table.
			qmarks := make([]string, bulkInsertCount)
			for i := range qmarks {
				qmarks[i] = "(?)"
			}
			qmarksStr := strings.Join(qmarks, ",")
			q := fmt.Sprintf("INSERT INTO %s (data) VALUES %s", tableName, qmarksStr)
			stmt, err := db.c.PrepareContext(ctx, q)
			if err != nil {
				log.Fatalf("Error preparing statement: %v", err)
			}

			for inserted := int64(0); inserted < numRows; inserted += int64(bulkInsertCount) {
				rows := <-rowsCh
				err = db.InsertRows(ctx, stmt, rows, bulkInsertCount)
				if err != nil {
					panic(err)
				}
				bar.Increment()
			}
		}(fmt.Sprintf("table%03d", i))
	}
	wg.Wait()

	bar.Finish()
}
