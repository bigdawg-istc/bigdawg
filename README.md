# BigDawg Middleware

manydbbench
===========

If you hit a maven bug on [1.7, not closed
in maven repository (~/.m2/repository) edit
 org/glassfish/jersey/project/2.19/project-2.19.pom
 to have [1.7,) instead of [1.7,

Might need maven3 on ubuntu 14+. See http://sysads.co.uk/2014/05/install-apache-maven-3-2-1-ubuntu-14-04/

Requires
----------
Java 7

Mvn 2

PostgreSQL

Prepare Eclipse (may need eclipse restart)
----------------
mvn -Declipse.workspace=[path to workspace] eclipse:add-maven-repo


mvn eclipse:eclipse

download dependencies
----------
mvn install -P XXX  

where XXX is [dev/test/prod] 

build jar
----------
mvn package -P XXX

run test server
---------
mvn exec:java -P XXX

test CURL post register a push alert
---------
curl -H "Content-Type: application/json" -X POST -d '{"query":"checkHeartRate","authorization":{}, "notifyURL":"http://localthost/notify2", "pushNotify":"true"}' http://localhost:8080/bigdawg/registeralert


test CURL post register a pull alert
----------
curl -H "Content-Type: application/json" -X POST -d '{"query":"checkHeartRate","authorization":{}}' http://localhost:8080/bigdawg/registeralert


test CURL get status
--------
curl  http://localhost:8080/bigdawg/status/x


invoke a new alert event from DB engine
--------
curl http://localhost:8080/bigdawg/alert/0

test CURL post query against PostgreSQL
--------
curl -v -H "Content-Type: application/json" -X POST -d '{"query":"RELATION(select * from mimic2v26.d_patients limit 5)","authorization":{},"tuplesPerPage":1,"pageNumber":1,"timestamp":"2012-04-23T18:25:43.511Z"}' http://localhost:8080/bigdawg/query

test CURL post against test server
--------
curl -v -H "Content-Type: application/json" -X POST -d '{"query":"RELATION(select * from mimic2v26.d_patients limit 5)","authorization":{},"tuplesPerPage":1,"pageNumber":1,"timestamp":"2012-04-23T18:25:43.511Z"}' http://128.52.183.245:8080/bigdawg/query

test CURL mit server @txe1-login.mit.edu
--------
curl -v -H "Content-Type: application/json" -X POST -d '{"query":"RELATION(select * from mimic2v26.d_patients limit 5)","authorization":{},"tuplesPerPage":1,"pageNumber":1,"timestamp":"2012-04-23T18:25:43.511Z"}' http://172.16.4.62:8080/bigdawg/query?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory

Myria query example
--------
curl -v -H "Content-Type: application/json" -X POST -d '{"query":"MYRIA(T1 = empty(x:int); T2 = [from T1 emit $0 as x]; store(T2, JustX);)","authorization":{},"tuplesPerPage":1,"pageNumber":1,"timestamp":"2012-04-23T18:25:43.511Z"}' http://localhost:8080/bigdawg/query

or

curl -v -H "Content-Type: application/json" -X POST -d '{"query":"MYRIA(T1 = empty(x:int); T2 = [from T1 emit $0 as x]; store(T2, JustX);)","authorization":{},"tuplesPerPage":1,"pageNumber":1,"timestamp":"2012-04-23T18:25:43.511Z"}' http://node-037:8090/bigdawg/query

D4M example
--------
curl -v -H "Content-Type: application/json" -X POST -d '{"query":"TEXT(classdb01 note_events_Tw T(^^patientID|01369,^^,:))","authorization":{},"tuplesPerPage":1,"pageNumber":1,"timestamp":"2012-04-23T18:25:43.511Z"}' http://172.16.4.139:8080/bigdawg/query

SciDB data loading
--------
curl  -H "Content-Type: application/json" -X POST -d '{"script":"/home/adam/Chicago/bigdawgmiddle/scripts/test_script/scidbLoad.sh","authorization":{},"dataLocation":"/home/gridsan/groups/istcdata/pipeline/scidb/data/ABP_0_1","flatArrayName":"ABP_0_1_flat","arrayName":"ABP_0"}' http://localhost:8080/bigdawg/load/scidb

maven profiles
--------
Building jar for test (big2: ssh ubuntu@128.52.183.84) and prod (big: ssh ubuntu@128.52.183.245) environemnts.
To run your local development environment run: 

mvn clean compile test -P dev

If you are bothered by an error for pom.xml in Eclipse, then you can go to Project->Properties->Maven and fill in the Active Maven Profiles (comma separated) field with "dev" word but then you won't be able to build the packages for the test and prod environments (the default dev profile overwrites all the settings).

Building packages for test and prod environments:
First run the packaging with tests (this is to check if everything is correct): mvn clean package -P dev

To build package for test environment run: mvn clean package -P test -DskipTests
go to project_dir/bigdawgmiddle/target and run: scp istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar ubuntu@128.52.183.84:/home/ubuntu/jars

To build package for prod environment run: mvn clean package -P prod -DskipTests
go to project_dir/bigdawgmiddle/target and run: scp istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar ubuntu@128.52.183.245:/home/ubuntu/jars

and finally run the application on the server:
java -jar /home/ubuntu/jars/istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar

checks:
test: curl -v -H "Content-Type: application/json" -X POST -d '{"query":"RELATION(select * from mimic2v26.d_patients limit 5)","authorization":{},"tuplesPerPage":1,"pageNumber":1,"timestamp":"2012-04-23T18:25:43.511Z"}' http://128.52.183.84:8080/bigdawg/query    

prod: curl -v -H "Content-Type: application/json" -X POST -d '{"query":"RELATION(select * from mimic2v26.d_patients limit 5)","authorization":{},"tuplesPerPage":1,"pageNumber":1,"timestamp":"2012-04-23T18:25:43.511Z"}' http://128.52.183.245:8080/bigdawg/query    

relay requests
--------
curl -H "Content-Type: application/json" -X POST -d '{"Query":"checkHeartRate","Authorization":"{}", "NotifyURL":"http://localhost:8008/results", "OneTime":"True","RelayURL":"http://cambridge.cs.pdx.edu:8080/test"}' http://localhost:8080/bigdawg/from

Gives: {"Response Code":200,"Status URL":"http://localhost:8888/success"}

The main input data (for us) is in RelayURL. We simply take the address from this attribute send a similar http request (the same data) but with the RelayURL attribute removed.

LOGS
--------
create database as pguser:

$psql -U pguser -d mimic2

**create database logs**

**create logs table** (current version is in the script folder)


RUN THE APP
--------
/usr/lib/jvm/java-1.7.0-openjdk/jre/bin/java -classpath "bigdawg-conf/:istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar" istc.bigdawg.Main

# bigdawg-conf contains the configuration files that should be adjusted to the current environment

SciDB on big2 VM
--------
sudo passwd scidb

su scidb

cd /opt/scidb/14.12/bin

./scidb.py status single_server

./scidb.py stop_all single_server

./scidb.py start_all single_server


Dealing with the MIT TXE server
--------

// password for the database: mimic01

cd /home/gridsan/groups/databases/mimic01

// folder where the server is running

cd /home/gridsan/groups/istcdata/technology/bigdawg

curl -v -H "Content-Type: application/json" -X POST -d '{"query":"RELATION(select * from mimic2v26.d_patients limit 5)","authorization":{},"tuplesPerPage":1,"pageNumber":1,"timestamp":"2012-04-23T18:25:43.511Z"}' http://172.16.4.61:8080/bigdawg/query

curl -H "Content-Type: application/json" -X POST -d '{"query":"checkHeartRate","authorization":{}, "pushNotify":"true"}' http://172.16.4.61:8080/bigdawg/registeralert

// run code from the BigDAWG bitbucket repo on the txe server

nohup mvn exec:java 2>&1 > bigdawg.log &


----------------

For demo of Phase 0.1:

Setting up PostgreSQL on local host, not for Catalog:

CREATE DATABASE mimic2;
\c mimic2
CREATE SCHEMA mimic2v26;
CREATE TABLE d_patients (
pid integer primary key,
name archer(10)
);
INSERT INTO d_patients VALUES(1, "John Smith");
INSERT INTO d_patients VALUES(2, "Jane Smith");
CREATE USER pguser PASSWORD 'test';
GRANT ALL ON ALL TABLES IN SCHEMA mimic2v26 TO pguser;

To run a query with one bdrel(...) layer: 

curl -v -H "Content-Type: application/json" -X POST -d '{"query":"RELATION(bdrel(select * from mimic2v26.d_patients limit 5);)","authorization":{},"tuplesPerPage":1,"pageNumber":1,"timestamp":"2012-04-23T18:25:43.511Z"}' http://localhost:8080/bigdawg/query


# For demo of Phase 0.2:

The main question is how we can handle many database instances. The idea is to store the meta-information (instance host, port, datbase name, etc.) in the catalog. The only connection information stored in the config file: bigdawgmiddle/src/main/resources/bigdawg-config.properties pertain to the PostgreSQL instance where the data for the catalog is stored. Additionally, the config file: bigdawgmiddle/src/main/resources/bigdawg-log4j.properties contains information about PostgreSQL instance where the logs are stored. 

### The current working version can be tested here:

```
#!bash

curl -v -H "Content-Type: application/json" -X POST -d '{"query":"RELATION(select * from mimic2v26.d_patients limit 6);","authorization":{},"tuplesPerPage":1,"pageNumber":1,"timestamp":"2012-04-23T18:25:43.511Z"}' http://madison.cs.uchicago.edu:8080/bigdawg/query
```


### Prepare environment with 2 PostgreSQL instances:
- go to bigdawgmiddle/installation (there are the scripts that we will use)
- mkdir data
- cd data
- download mimic2.pgd to the data directory from [https://app.box.com/s/8by2c36m8bwxl9654bwf3mttdt25uu4k](Link URL)
- run script: **bash setup.sh** (this script should be executed only once)
- to stop the PostgreSQL instances run: bash stop_postgres.sh
- to start the PostgreSQL instances run: bash start_postgres.sh

### Additionally, you can change pom.xml and try migrating data from postgres1 to postgres2:
- go to bigdawgmiddle/
- in pom.xml uncomment: <bigDawg.main.class>istc.bigdawg.migration.FromPostgresToPostgres</bigDawg.main.class>
- in pom.xml comment: <bigDawg.main.class>istc.bigdawg.Main</bigDawg.main.class>
- run: mvn clean compile -P dev
- run: mvn exec:java

```
#!bash


0    [main] INFO  istc.bigdawg.LoggerSetup  - Starting application. Logging was configured!
Migrating data from PostgreSQL to PostgreSQL
143  [main] DEBUG istc.bigdawg.migration.FromPostgresToPostgres  - Number of extracted rows: 143 Number of loaded rows: 143
```


### The last script creates 2 PostgreSQL instances:
- postgres1 port: 5431 in bigdawgmiddle/installation/Downloads/postgres1
- postgres2 port: 5430 in bigdawgmiddle/installation/Downloads/postgres2

## You can access a few databases in the following way:
### logs: 
- go to bigdawgmiddle/installation/Downloads/postgres1/bin
- ./psql -p 5431 -d logs
- select * from logs order by time desc;


```
#!sql

 user_id |          time           |                    logger                     | level |                                                                                message                    
                                                            
---------+-------------------------+-----------------------------------------------+-------+-----------------------------------------------------------------------------------------------------------
------------------------------------------------------------
         | 2015-12-23 12:17:24.358 | istc.bigdawg.migration.FromPostgresToPostgres | DEBUG | Number of extracted rows: 143 Number of loaded rows: 143
         | 2015-12-23 12:17:24.215 | istc.bigdawg.LoggerSetup                      | INFO  | Starting application. Logging was configured!
         | 2015-12-23 11:39:39.607 | istc.bigdawg.postgresql.PostgreSQLHandler     | INFO  | format TABLE Java time milliseconds: 0,
         | 2015-12-23 11:39:39.604 | istc.bigdawg.postgresql.PostgreSQLHandler     | INFO  | PostgreSQL query execution time milliseconds: 21,
         | 2015-12-23 11:39:39.577 | istc.bigdawg.query.QueryClient                | INFO  | istream: {"query":"RELATION(select * from mimic2v26.d_patients limit 6);","authorization":{},"tuplesPerPag
e":1,"pageNumber":1,"timestamp":"2012-04-23T18:25:43.511Z"}
(5 rows)

```


### bigdawg_catalog:
- go to bigdawgmiddle/installation/Downloads/postgres1/bin
- ./psql -p 5431 -d bigdawg_catalog
- select * from catalog.engines;

```
#!sql

 eid |   name    |   host    | port |              connection_properties              
-----+-----------+-----------+------+-------------------------------------------------
   1 | postgres1 | localhost | 5431 | engine for bigdawg catalog data and mimic2 data
   2 | postgres2 | localhost | 5430 | main engine for mimic2_copy data
(2 rows)
```


- select * from catalog.databases;
```
#!sql

 dbid | engine_id |      name       |  userid  | password 
------+-----------+-----------------+----------+----------
    1 |         1 | bigdawg_catalog | postgres | test
    2 |         1 | mimic2          | pguser   | test
    3 |         2 | mimic2_copy     | pguser   | test
(3 rows)

```


### mimic2:
- go to bigdawgmiddle/installation/Downloads/postgres1/bin
- ./psql -p 5431 -d mimic2
- select * from mimic2v26.d_patients limit 6;

### mimic2_copy:
- go to bigdawgmiddle/installation/Downloads/postgres2/bin
- ./psql -p 5430 -d mimic2_copy
- select * from mimic2v26.d_patients limit 6;