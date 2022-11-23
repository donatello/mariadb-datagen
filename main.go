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

	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
)

var (
	dsn     string
	testDB  string
	sizeStr string

	sizeMB  int64
	threads int
)

func init() {
	flag.StringVar(&dsn, "dsn", "root:secretpassword@tcp(localhost)/mysql", "MariaDB DSN")
	flag.StringVar(&testDB, "test-db", "", "Database to create")
	flag.StringVar(&sizeStr, "size", "1G", "Database size to generate ('3' or '3G' => 3GB, '10M' => 10MB, '1T' => 1TB)")
	flag.IntVar(&threads, "threads", 1, "Number of threads to use")
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

func (db *DB) InsertRows(ctx context.Context, name string, count, rlen int) error {
	rows := make([]any, count)
	qmarks := make([]string, count)
	rlen -= 8 // 8 bytes of data will be present for the id already.
	for i := range rows {
		rows[i] = GenData(rlen)
		qmarks[i] = "(?)"
	}

	qmarksStr := strings.Join(qmarks, ",")
	query := fmt.Sprintf("INSERT INTO %s (data) VALUES %s", name, qmarksStr)
	stmt, err := db.c.PrepareContext(ctx, query)
	if err != nil {
		return err
	}

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
	log.Printf("Created database. Populating...")

	const (
		rowSize      = 2048 // 2KiB
		randDataSize = rowSize - 8
	)

	var (
		totalDataSizeBytes float64 = float64(sizeMB * 1000 * 1000)
		// Data per table:
		perThreadBytes float64 = math.Ceil(totalDataSizeBytes / float64(threads))
		// Row count per table:
		numRows int64 = int64(math.Ceil(perThreadBytes / rowSize))
	)

	fmt.Printf("Generating %d table(s) with %d rows (2KiB each) per table\n", threads, numRows)

	wg := sync.WaitGroup{}
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func(tableName string) {
			defer wg.Done()
			err := db.CreateTable(ctx, tableName)
			if err != nil {
				log.Fatalf("Error initializing database: %v", err)
				panic(err)
			}

			remainingRows := numRows
			for remainingRows > 0 {
				rowCount := 4000
				if remainingRows < int64(rowCount) {
					rowCount = int(remainingRows)
				}
				err = db.InsertRows(ctx, tableName, rowCount, rowSize)
				if err != nil {
					panic(err)
				}
				remainingRows -= int64(rowCount)
			}
		}(fmt.Sprintf("table%03d", i))
	}
	wg.Wait()

	fmt.Printf("done.\n")
}
