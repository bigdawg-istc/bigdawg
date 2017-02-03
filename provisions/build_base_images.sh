#!/bin/bash

# This script produces baseline images with the Middleware but no data
# Login to Dockerhub repository first ("docker login <username>") so that "docker push" works

echo
echo "======================================================"
echo "===== Packaging the Middleware jar and resources ====="
echo "======================================================"

mvn package -P mit -DskipTests -f ../pom.xml

echo
echo "==================================================="
echo "===== Copying artifacts to docker directories ====="
echo "==================================================="

cp ../target/istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar postgres
cp ../target/istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar scidb
cp ../target/istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar accumulo
cp ../src/main/resources/PostgresParserTerms.csv postgres
cp ../src/main/resources/PostgresParserTerms.csv scidb
cp ../src/main/resources/PostgresParserTerms.csv accumulo
cp ../src/main/resources/SciDBParserTerms.csv postgres
cp ../src/main/resources/SciDBParserTerms.csv scidb
cp ../src/main/resources/SciDBParserTerms.csv accumulo

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
