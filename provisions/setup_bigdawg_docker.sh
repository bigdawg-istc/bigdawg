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

echo
echo "========================================"
echo "===== Pulling images from Dockerhub====="
echo "========================================"
echo "==> postgres"
docker pull bigdawg/postgres
echo "==> scidb"
docker pull bigdawg/scidb
echo "==> accumulo"
docker pull bigdawg/accumulo

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
echo "==> postgres-data2"
docker rm -f bigdawg-postgres-data2
docker run -d --net=bigdawg -h bigdawg-postgres-data2 -p 5402:5402 -e "PGPORT=5402" -e "BDHOST=bigdawg-postgres-data2" --name bigdawg-postgres-data2 bigdawg/postgres
echo "==> scidb"
docker pull bigdawg/scidb
docker run -d --net=bigdawg -h bigdawg-scidb-data -p 49901:22 -p 8000:8000 --expose=5432 -p 1239:1239 --name bigdawg-scidb bigdawg/scidb
echo "==> accumulo"
docker pull bigdawg/accumulo
accumulo/start_accumulo_cluster.sh

echo
echo "========================"
echo "===== Loading data ====="
echo "========================"

# postgres-catalog
docker cp cluster_setup/postgres-catalog/bdsetup bigdawg-postgres-catalog:/
docker exec bigdawg-postgres-catalog /bdsetup/setup.sh

# Download the mimic2 dataset
if [ -f "mimic2_flatfiles.tar.gz" ]
then
	echo "Mimic data already exists. Skipping download"
else
	echo "Downloading the mimic2 dataset"
	curl -o mimic2_flatfiles.tar.gz --create-dirs https://physionet.org/mimic2/demo/mimic2_flatfiles.tar.gz
fi

# postgres-data1
docker cp cluster_setup/postgres-data1/bdsetup bigdawg-postgres-data1:/
docker cp mimic2_flatfiles.tar.gz bigdawg-postgres-data1:/bdsetup/
docker exec --user=root bigdawg-postgres-data1 /bdsetup/setup.sh

# postgres-data2
docker cp cluster_setup/postgres-data2/bdsetup bigdawg-postgres-data2:/
docker cp mimic2_flatfiles.tar.gz bigdawg-postgres-data2:/bdsetup/
docker exec --user=root bigdawg-postgres-data2 /bdsetup/setup.sh

# accumulo
docker cp cluster_setup/accumulo-data/bdsetup bigdawg-accumulo-zookeeper:/
docker exec bigdawg-accumulo-zookeeper /bdsetup/insertData.sh

echo
echo "================="
echo "===== Done. ====="
echo "================="