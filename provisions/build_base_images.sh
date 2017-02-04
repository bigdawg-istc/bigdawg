#!/bin/bash

# This script produces baseline images with the Middleware but no data
# Login to Dockerhub repository first ("docker login <username>") so that "docker push" works
#
# Remove all images: docker rmi -f $(docker images -a -q)
# Remove all containers: docker rm $(docker ps -a -q)

echo
echo "========================================"
echo "===== Packaging the Middleware jar ====="
echo "========================================"

mvn package -P mit -DskipTests -f ../pom.xml

echo
echo "==================================================="
echo "===== Copying artifacts to docker directories ====="
echo "==================================================="

cp ../target/istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar postgres
cp ../target/istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar scidb
cp ../target/istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar accumulo
chmod +r postgres/istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar
chmod +r scidb/istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar
chmod +r accumulo/istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar
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

echo "==> scidb"
docker build --rm -t bigdawg/scidb scidb/

echo "==> accumulo"
docker build --rm -t bigdawg/accumulo-base docker-builds/accumulo/
docker build --rm -t bigdawg/accumulo accumulo/

echo "==> pushing images to dockerhub"
docker push bigdawg/postgres
docker push bigdawg/scidb
docker push bigdawg/accumulo-base
docker push bigdawg/accumulo

echo
echo "================="
echo "===== Done. ====="
echo "================="
