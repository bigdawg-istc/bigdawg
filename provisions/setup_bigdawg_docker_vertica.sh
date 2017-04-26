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
echo "==> vertica"
docker build --rm -t bigdawg/vertica vertica/

echo
echo "============================="
echo "===== Running containers====="
echo "============================="
echo "==> postgres-catalog"
docker rm -f bigdawg-postgres-catalog
docker run -d --net=bigdawg -h bigdawg-postgres-catalog -p 5400:5400 -p 8080:8080 -e "PGPORT=5400" -e "BDHOST=bigdawg-postgres-catalog" --name bigdawg-postgres-catalog bigdawg/postgres
echo "==> postgres-data1"
docker rm -f bigdawg-postgres-data1
docker run -d --net=bigdawg -h bigdawg-postgres-data1 -p 5401:5401 -e "PGPORT=5401" -e "BDHOST=bigdawg-postgres-data1" --name bigdawg-postgres-data1 bigdawg/postgres
# docker rm -f bigdawg-postgres-data2
# docker run -d --net=bigdawg -h bigdawg-postgres-data2 -p 5402:5402 -e "PGPORT=5402" -e "BDHOST=bigdawg-postgres-data2" --name bigdawg-postgres-data2 bigdawg/postgres

echo "==> vertica-data"
docker rm -f bigdawg-vertica-data
docker run -d --net=bigdawg -h bigdawg-vertica-data -p 5433:5433 --name bigdawg-vertica-data bigdawg/vertica


# echo
# echo "========================"
# echo "===== Loading data ====="
# echo "========================"

# # Download the mimic2 dataset
if [ -f "mimic2_flatfiles.tar.gz" ]
then
       echo "Mimic data already exists. Skipping download"
else
       echo "Downloading the mimic2 dataset"
       curl -o mimic2_flatfiles.tar.gz --create-dirs https://physionet.org/mimic2/demo/mimic2_flatfiles.tar.gz
fi

# postgres-catalog
docker exec -u root bigdawg-postgres-catalog mkdir -p /src/main/resources
docker cp ../src/main/resources/PostgresParserTerms.csv bigdawg-postgres-catalog:/src/main/resources
docker cp ../src/main/resources/SciDBParserTerms.csv bigdawg-postgres-catalog:/src/main/resources
docker cp cluster_setup/postgres-catalog/bdsetup bigdawg-postgres-catalog:/
docker exec bigdawg-postgres-catalog /bdsetup/setup_vertica.sh

# postgres-data1
docker cp cluster_setup/postgres-data1/bdsetup bigdawg-postgres-data1:/
docker cp mimic2_flatfiles.tar.gz bigdawg-postgres-data1:/bdsetup/
docker exec --user=root bigdawg-postgres-data1 /bdsetup/setup.sh

# # postgres-data2
# docker cp cluster_setup/postgres-data2/bdsetup bigdawg-postgres-data2:/
# docker cp mimic2_flatfiles.tar.gz bigdawg-postgres-data2:/bdsetup/
# docker exec --user=root bigdawg-postgres-data2 /bdsetup/setup.sh

# vertica-data
docker cp cluster_setup/vertica-data/bdsetup bigdawg-vertica-data:/home/dbadmin/
docker exec --user=root bigdawg-vertica-data chmod +x /home/dbadmin/bdsetup/setup.sh
docker exec --user=dbadmin bigdawg-vertica-data /home/dbadmin/bdsetup/setup.sh

# # scidb
# docker cp cluster_setup/scidb-data/bdsetup bigdawg-scidb-data:/home/scidb/
# docker exec bigdawg-scidb-data /home/scidb/bdsetup/setup.sh

# # accumulo
# docker cp cluster_setup/accumulo-data/bdsetup bigdawg-accumulo-zookeeper:/
# docker exec bigdawg-accumulo-zookeeper /bdsetup/insertData.sh

echo
echo "======================================="
echo "===== Starting BigDAWG Middleware ====="
echo "======================================="
# docker exec -d bigdawg-scidb-data java -classpath "istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar" istc.bigdawg.Main bigdawg-scidb-data
docker exec bigdawg-postgres-catalog java -classpath "istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar" istc.bigdawg.Main bigdawg-postgres-catalog

echo
echo "================="
echo "===== Done. ====="
echo "================="

SELECT attrelid, attname, format_type(atttypid, atttypmod) AS type, atttypid, atttypmod FROM pg_catalog.pg_attribute WHERE NOT attisdropped AND attrelid = 'd_patients'::regclass AND atttypid NOT IN (26,27,28,29) ORDER BY attnum;"
