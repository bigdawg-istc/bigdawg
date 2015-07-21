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
