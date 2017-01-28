#!/bin/bash

# Login to Dockerhub repository using "docker login <username>" so that "docker push" works below.
# This script handles the build and push of base containers
# Base containers have no data, but have the middleware jar

echo
echo "========================================"
echo "===== Packaging the Middleware jar ====="
echo "========================================"
#mvn clean -f ../pom.xml 
#mvn resources:resources -f ../pom.xml
mvn package -P mit -DskipTests -f ../pom.xml
cp ../target/istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar postgres
cp ../target/istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar scidb

echo
echo "===================================================="
echo "===== Building images and pushing to dockerhub ====="
echo "===================================================="
echo "==> postgres"
docker build --rm -t bigdawg/postgres postgres/
docker push bigdawg/postgres
echo "==> scidb"
docker build --rm -t bigdawg/scidb scidb/
docker push bigdawg/scidb
echo "==> accumulo"
docker build --rm -t bigdawg/accumulo-base docker-builds/accumulo/
docker push bigdawg/accumulo-base
docker build --rm -t bigdawg/accumulo accumulo/
docker push bigdawg/accumulo

echo
echo "================="
echo "===== Done. ====="
echo "================="
