#!/bin/bash

# This script does the following:
# 	- docker initialization: creates the "bigdawg" docker network if not already created
# 	- image pull: pulls postgres, scidb, and accumulo base images from dockerhub
# 	- container run: runs the above containers
# 	- data prep: download and insert data into running containers

echo
echo "=============================================================="
echo "===== Creating the bigdawg docker network if not present ====="
echo "=============================================================="
$(docker network inspect bigdawg &>/dev/null) || {   docker network create bigdawg; }

rm postgres/istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar
cp ../target/istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar postgres

# echo
# echo "========================================"
# echo "===== Pulling images from Dockerhub====="
# echo "========================================"
# echo "==> postgres"
# docker pull bigdawg/postgres
# echo "==> scidb"
# docker pull bigdawg/scidb
echo "==> postgres"
docker build --rm -t bigdawg/postgres postgres/

echo
echo "============================="
echo "===== Running containers====="
echo "============================="
echo "==> postgres-catalog"
docker rm -f bigdawg-postgres-catalog
docker run -d --net=bigdawg -h bigdawg-postgres-catalog -p 5400:5400 -p 8080:8080 -e "PGPORT=5400" -e "BDHOST=bigdawg-postgres-catalog" --name bigdawg-postgres-catalog bigdawg/postgres
echo "==> postgres-data1"
docker rm -f bigdawg-postgres-data1
docker run -d --net=bigdawg -h bigdawg-postgres-tpch -p 5401:5401 -e "PGPORT=5401" -e "BDHOST=bigdawg-postgres-tpch" --name bigdawg-postgres-tpch bigdawg/postgres

# echo
echo "========================"
echo "===== Loading data ====="
echo "========================"

# postgres-catalog
docker exec -u root bigdawg-postgres-catalog mkdir -p /src/main/resources
docker cp ../src/main/resources/PostgresParserTerms.csv bigdawg-postgres-catalog:/src/main/resources
docker cp ../src/main/resources/SciDBParserTerms.csv bigdawg-postgres-catalog:/src/main/resources
docker cp cluster_setup/postgres-catalog/bdsetup bigdawg-postgres-catalog:/
docker exec bigdawg-postgres-catalog /bdsetup/setup_tpch.sh

# postgres-data1
docker cp cluster_setup/postgres-tpch/bdsetup bigdawg-postgres-tpch:/
docker cp pg_tpch bigdawg-postgres-tpch:/bdsetup/
docker cp dbgen bigdawg-postgres-tpch:/bdsetup/
docker exec --user=root bigdawg-postgres-tpch /bdsetup/setup.sh


echo
echo "======================================="
echo "===== Starting BigDAWG Middleware ====="
echo "======================================="
docker exec bigdawg-postgres-catalog java -classpath "istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar" istc.bigdawg.Main bigdawg-postgres-catalog

echo
echo "================="
echo "===== Done. ====="
echo "================="
