# Building

Building instructions as of 10/18/2019

Using Ubuntu 18.04.1 LTS (for other distros YMMV)

# Install mvn, java

Maven install example:

```bash
sudo apt install -y maven
mvn --version
```

Should seeL

```bash
Apache Maven 3.6.0
Maven home: /usr/share/maven
Java version: 11.0.3, vendor: Oracle Corporation, runtime: /usr/lib/jvm/java-11-openjdk-amd64
Default locale: en, platform encoding: UTF-8
OS name: "linux", version: "4.4.0-17763-microsoft", arch: "amd64", family: "unix"
```

# Install Vertica JDBC Driver

Download 8.0.1 jar from: [https://www.vertica.com/download/vertica/client-drivers/](https://www.vertica.com/download/vertica/client-drivers/)

Install the jar file:

```bash
# where $HOME/Downloads is the path to the jar file you downloaded.
mvn install:install-file -Dfile=$HOME/Downloads/vertica-jdbc-8.0.1-6.jar -DgroupId=com.vertica -DartifactId=vjdbc8 -Dversion=8.0.1 -Dpackaging=jar
```

# Build the JAR

Run this command from within the bigdawg-istc source directory (in the root level of the directory):

```bash
# this builds the MIT profile which is used in the docker containers
mvn package -P mit -DskipTests
```

## (Optional) Testing and Distributing to the docker containers

If not running, first run [provisions/setup_bigdawg_docker.sh](provisions/setup_bigdawg_docker.sh)

```bash
provisions/setup_bigdawg_docker.sh
# This will take a while to start up.
```

You should see something like:

```bash
2019-10-19 04:51:47,795 0 istc.bigdawg.LoggerSetup.setLogging(LoggerSetup.java:47) [main] null INFO  istc.bigdawg.LoggerSetup - Logging was configured!
2019-10-19 04:51:47,857 62 istc.bigdawg.Main.main(Main.java:97) [main] null INFO  istc.bigdawg.Main - Starting application ...
2019-10-19 04:51:47,860 65 istc.bigdawg.Main.main(Main.java:100) [main] null INFO  istc.bigdawg.Main - Connecting to catalog
Connecting to catalog:
==>> jdbc:postgresql://bigdawg-postgres-catalog:5400/bigdawg_catalog
==>> postgres
==>> test
2019-10-19 04:51:47,877 82 istc.bigdawg.Main.main(Main.java:103) [main] null INFO  istc.bigdawg.Main - Checking registered database connections
2019-10-19 04:51:47,979 184 istc.bigdawg.Main.main(Main.java:119) [main] null DEBUG istc.bigdawg.Main - args 0: bigdawg-postgres-catalog
2019-10-19 04:51:47,979 184 istc.bigdawg.network.NetworkIn.receive(NetworkIn.java:29) [pool-2-thread-1] null DEBUG istc.bigdawg.network.NetworkIn - network in: start listening for requests
2019-10-19 04:51:47,982 187 istc.bigdawg.Main.startServer(Main.java:52) [main] null INFO  istc.bigdawg.Main - base uri: http://bigdawg-postgres-catalog:8080/bigdawg/
Starting HTTP server on: http://bigdawg-postgres-catalog:8080/bigdawg/
2019-10-19 04:51:48,033 238 istc.bigdawg.network.NetworkIn.receive(NetworkIn.java:39) [pool-2-thread-1] null DEBUG istc.bigdawg.network.NetworkIn - tcp://*:9991
2019-10-19 04:51:48,044 249 istc.bigdawg.network.NetworkIn.receive(NetworkIn.java:43) [pool-2-thread-1] null DEBUG istc.bigdawg.network.NetworkIn - Wait for the next request from a client ...
Oct 19, 2019 4:51:55 AM org.glassfish.grizzly.http.server.NetworkListener start
INFO: Started listener bound to [bigdawg-postgres-catalog:8080]
Oct 19, 2019 4:51:55 AM org.glassfish.grizzly.http.server.HttpServer start
INFO: [HttpServer] Started.
Jersey app started with WADL available at http://bigdawg-postgres-catalog:8080/bigdawg/application.wadl
Hit enter to stop it...
```

Then stop the catalog process:

```bash
<Hit your Return / Enter key in the window running the catalog>
```

Verify that the conatiners are running:

```bash
docker ps
```

You should see something like this:

```bash
CONTAINER ID        IMAGE               COMMAND                  CREATED             STATUS              PORTS                                              NAMES
dfeada517961        bigdawg/accumulo    "/usr/bin/supervisor…"   27 hours ago        Up 27 hours         0.0.0.0:42424->42424/tcp                           bigdawg-accumulo-proxy
18b520536493        bigdawg/accumulo    "/usr/bin/supervisor…"   27 hours ago        Up 27 hours         0.0.0.0:9999->9999/tcp, 0.0.0.0:50095->50095/tcp   bigdawg-accumulo-master
9fb8b3164c75        bigdawg/accumulo    "/usr/bin/supervisor…"   27 hours ago        Up 27 hours         0.0.0.0:9997->9997/tcp                             bigdawg-accumulo-tserver0
0dadf0858c01        bigdawg/accumulo    "/usr/bin/supervisor…"   27 hours ago        Up 27 hours         0.0.0.0:2181->2181/tcp                             bigdawg-accumulo-zookeeper
9b286637072e        bigdawg/accumulo    "/usr/bin/supervisor…"   27 hours ago        Up 27 hours                                                            bigdawg-accumulo-namenode
2e391253f505        bigdawg/scidb       "/bin/sh -c /start_s…"   27 hours ago        Up 27 hours         0.0.0.0:1239->1239/tcp                             bigdawg-scidb-data
5e9a6e73fe2e        bigdawg/postgres    "/bin/sh -c /start_s…"   27 hours ago        Up 27 hours         0.0.0.0:5402->5402/tcp                             bigdawg-postgres-data2
6ef3ae2601ff        bigdawg/postgres    "/bin/sh -c /start_s…"   27 hours ago        Up 27 hours         0.0.0.0:5401->5401/tcp                             bigdawg-postgres-data1
33953903bb67        bigdawg/postgres    "/bin/sh -c /start_s…"   27 hours ago        Up 27 hours         0.0.0.0:5400->5400/tcp, 0.0.0.0:8080->8080/tcp     bigdawg-postgres-catalog
```

Now copy the newly built jar file into the containers:

```bash
# The below command should be executed from within the bigdawg-istc source directory
# The bigdawg docker containers should be running (from [provisions/setup_bigdawg_docker.sh](provisions/setup_bigdawg_docker.sh))
for i in $(docker ps | grep -v CONTAINER | awk '{print $1}'); do docker cp $PWD/target/istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar $i:.; done
```

Now to restart everywhere:

```bash
docker exec bigdawg-scidb-data bash -c "ps auxww | grep bigdawg-scidb-data | grep -v grep  | awk '{print \$2}' | xargs kill -9";
docker exec -d bigdawg-scidb-data java -classpath "istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar" istc.bigdawg.Main bigdawg-scidb-data
docker exec bigdawg-postgres-data1 bash -c "ps auxww | grep bigdawg-postgres-data1 | grep -v grep  | awk '{print \$2}' | xargs kill -9";
docker exec -d bigdawg-postgres-data1 java -classpath "istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar" istc.bigdawg.Main bigdawg-postgres-data1
docker exec bigdawg-postgres-data2 bash -c "ps auxww | grep bigdawg-postgres-data2 | grep -v grep  | awk '{print \$2}' | xargs kill -9";
docker exec -d bigdawg-postgres-data2 java -classpath "istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar" istc.bigdawg.Main bigdawg-postgres-data2
docker exec bigdawg-accumulo-zookeeper bash -c "ps auxww | grep bigdawg-accumulo-zookeeper | grep -v grep  | awk '{print \$2}' | xargs kill -9";
docker exec -d bigdawg-accumulo-zookeeper java -classpath "istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar" istc.bigdawg.Main bigdawg-accumulo-zookeeper
```

Also stop and restart the catalog:

```bash
# after hitting any key on the window running the catalog
docker exec -it bigdawg-postgres-catalog java -classpath "istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar" istc.bigdawg.Main bigdawg-postgres-catalog
```

## (Optional) Building cmigrator.

Building the cmigrator program ([src/main/cmigrator](src/main/cmigrator)):

Edit src/main/cmigrator/CMakeLists.txt, comment out the line(s) related to testing:

```cmake
# check: https://github.com/kaizouman/gtest-cmake-example/blob/master/CMakeLists.txt
cmake_minimum_required(VERSION 2.8)

project(cmigrator)

# enable_testing()

add_subdirectory(src)
# add_subdirectory(test)
```

Follow the instructions in the README.md.

### Building cmigrator inside of one of the containers

Tar up the cmigrator directory:

```bash
cd src/main; tar czf cmigrator.tar.gz cmigrator
```

Copy into the container:

```bash
docker cp cmigrator.tar.gz bigdawg-scidb-data:.
docker exec -it bigdawg-scidb-data bash
cd src/main; tar xzf /cmigrator.tar.gz
cd cmigrator
# edit CMakeLists.txt similar as above
```

```bash
# now install boost:
wget https://sourceforge.net/projects/boost/files/boost/1.60.0/boost_1_60_0.tar.gz/download -O boost_1_60_0.tar.gz
tar xzvf boost_1_60_0.tar.gz
cd boost_1_60_0/
apt-get update
apt-get install -y build-essential g++ python-dev autotools-dev libicu-dev build-essential libbz2-dev
./bootstrap.sh --prefix=/usr/local
./b2
./b2 install
```

```bash
# Set environment variables
export LIBS="-L/usr/local/lib":$LIBS
export CPPFLAGS="-I/usr/local/include/boost"
export BOOST_INCLUDEDIR=/usr/local/include/boost/
export BOOST_LIBRARYDIR=/usr/local/lib/
export BOOST_ROOT=/home/adam/Downloads/boost_1_60_0
export INCLUDE="/usr/local/include/boost/:$INCLUDE"
export LIBRARY_PATH="/usr/local/lib/:$LIBRARY_PATH"
# now build cmigrator:
cd ..
mkdir build
cd build
apt install -y cmake
cmake ..
make
```

## (Optional) Saving changes to docker containers

Suggested route:

Run from the root of the bigdawg-istc source directory.

Postgres update:

```bash
docker rm -f docker-postgres-catalog
docker run -d -h bigdawg-postgres-catalog --net=bigdawg -p 5400:5400 -p 8080:8080 -e "PGPORT=5400" -e "BDHOST=bigdawg-postgres-catalog" --name bigdawg-postgres-catalog bigdawg/postgres
docker cp $PWD/target/istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar bigdawg-catalog-postgres:.
# YYYYMMDD should be some date like 20191018 or similar, this is only a suggestion
docker commit bigdawg-catalog-postgres bigdawg/postgres:YYYYMMDD
docker push bigdawg/postgres:YYYYMMDD
docker tag bigdawg/postgres:YYYYMMDD bigdawg/postgres:latest
docker push bigdawg/postgres:latest
```

SciDB update:

```bash
docker rm -f docker-scidb-data
docker run -d -h bigdawg-scidb-data --net=bigdawg -p 1239:1239 --name bigdawg-scidb-data bigdawg/scidb
docker cp $PWD/target/istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar bigdawg-scidb-data:.
# YYYYMMDD should be some date like 20191018 or similar, this is only a suggestion
docker commit bigdawg-scidb-data bigdawg/scidb:YYYYMMDD
docker push bigdawg/scidb:YYYYMMDD
docker tag bigdawg/scidb:YYYYMMDD bigdawg/scidb:latest
docker push bigdawg/scidb:latest
```

Accumulo update:

```bash
docker rm -f docker-accumulo-zookeeper
docker run -d --net=bigdawg \
              --hostname=zookeeper.docker.local \
              --net-alias=zookeeper.docker.local \
              --name=bigdawg-accumulo-zookeeper \
              -p 2181:2181 \
              -e 'SVCLIST=zookeeper' \
              bigdawg/accumulo /usr/bin/supervisord -n
docker cp $PWD/target/istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar bigdawg-accumulo-zookeeper:.
# YYYYMMDD should be some date like 20191018 or similar, this is only a suggestion
docker commit bigdawg-accumulo-zookeeper bigdawg/accumulo:YYYYMMDD
docker push bigdawg/accumulo:YYYYMMDD
docker tag bigdawg/accumulo:YYYYMMDD bigdawg/accumulo:latest
docker push bigdawg/accumulo:latest
```
