# Data generator for MariaDB

## Setting up DB for testing

``` sh
# Server
$ docker run --detach --name some-mariadb \
     -v $PWD/mariadbdata:/var/lib/mysql \
     --env MYSQL_USER=mariauser \
     --env MYSQL_PASSWORD=mariauser123 \
     --env MYSQL_ROOT_PASSWORD=secretpassword  \
     --publish 3306:3306/tcp \
     docker.io/library/mariadb:10.4 \
     --innodb_buffer_pool_size=8G --innodb_log_file_size=8G
``` 

To connect to the server, use the docker container IP ([doc](https://mariadb.com/kb/en/installing-and-using-mariadb-via-docker/)) - localhost does not work.


``` sh
# Client
$ docker run -it --rm \
     docker.io/library/mariadb:10.4 \
     mysql -h 172.17.0.2 -u root -p


```

## Running the tool

``` sh
./mariadb-datagen -test-db x13 -threads 16 -size 16G
2022/11/22 13:59:45 Created database. Populating...
Generating 16 table(s) with 488282 rows (2KiB each) per table
done.

```
