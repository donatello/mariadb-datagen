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
     docker.io/library/mariadb:10.4

``` 

To connect to the server, use the docker container IP ([doc](https://mariadb.com/kb/en/installing-and-using-mariadb-via-docker/)) - localhost does not work.


``` sh
# Client
$ docker run -it --rm \
     docker.io/library/mariadb:10.4 \
     mysql -h 172.17.0.2 -u root -p


```

