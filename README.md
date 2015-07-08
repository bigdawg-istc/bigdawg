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
mvn -Declipse.workspace=[path to workspace] eclipse:add-maven-repo <br>
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

test CURL post register
---------
curl -H "Content-Type: application/json" -X POST -d '{"query":"check heart rate","authorization":{}, "notifyURL":"http://localthost/notify"}' http://localhost:8080/bigdawg/registeralert


test CURL get status
--------
curl  http://localhost:8080/bigdawg/status/x 
