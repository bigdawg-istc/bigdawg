# BigDawg Middleware

manydbbench
===========

If you hit a maven bug on [1.7, not closed
in maven repository (~/.m2/repository) edit
 org/glassfish/jersey/project/2.19/project-2.19.pom
 to have [1.7,) instead of [1.7,

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
mvn install

build jar
----------
mvn package

run test server
---------
mvn exec:java

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
curl -v -H "Content-Type: application/json" -X POST -d '{"query":"select * from mimic2v26.d_patients limit 5","authorization":{},"tuplesPerPage":1,"pageNumber":1,"timestamp":"2012-04-23T18:25:43.511Z"}' http://localhost:8080/bigdawg/query

test CURL post against test server
--------
curl -v -H "Content-Type: application/json" -X POST -d '{"query":"RELATION(select * from mimic2v26.d_patients limit 5)","authorization":{},"tuplesPerPage":1,"pageNumber":1,"timestamp":"2012-04-23T18:25:43.511Z"}' http://128.52.183.245:8080/bigdawg/query

test CURL mit server @txe1-login.mit.edu
--------
curl -v -H "Content-Type: application/json" -X POST -d '{"query":"RELATION(select * from mimic2v26.d_patients limit 5)","authorization":{},"tuplesPerPage":1,"pageNumber":1,"timestamp":"2012-04-23T18:25:43.511Z"}' http://172.16.4.62:8080/bigdawg/query?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory

maven profiles
--------
Building jar for test (big2: ssh ubuntu@128.52.183.84) and prod (big: ssh ubuntu@128.52.183.245) environemnts.
To run your local development environment run: 

mvn clean compile test -P dev

If you are bothered by an error for pom.xml in Eclipse, then you can go to Project->Properties->Maven and fill in the Active Maven Profiles (comma separated) field with "dev" word but then you won't be able to build the packages for the test and prod environments (the default dev profile will overwrite all the settings).

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