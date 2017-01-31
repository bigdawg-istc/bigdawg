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
docker run -d --net=bigdawg -h bigdawg-scidb-data -p 9999:9999 -p 49901:22 -p 8000:8000 --expose=5432 -p 1239:1239 --name bigdawg-scidb-data bigdawg/scidb
echo "==> accumulo"
docker pull bigdawg/accumulo

# Note about accumulo hostnames: 
# Container hostnames must match those configured in docker-builds/accumulo/accumulo.conf/ "slaves" and "masters"
tag=bigdawg/accumulo
container_name=bigdawg-accumulo
network_name=bigdawg

echo "starting hdfs"
docker run -d --net=${network_name} \
              --hostname=namenode.docker.local \
              --net-alias=namenode.docker.local \
              --name=${container_name}-namenode \
              -e "SVCLIST=namenode,datanode" \
              ${tag} /usr/bin/supervisord -n

echo "start zookeeper"
docker run -d --net=${network_name} \
              --hostname=zookeeper.docker.local \
              --net-alias=zookeeper.docker.local \
              --name=${container_name}-zookeeper \
              -p 2181:2181 \
              -e 'SVCLIST=zookeeper' \
              ${tag} /usr/bin/supervisord -n

echo "hdfs needs a moment to become available"
sleep 10
echo "initializing Accumulo instance"
docker exec ${container_name}-namenode /usr/lib/accumulo/bin/init_accumulo.sh

echo "start tserver0"
docker run -d --net=${network_name} \
              --hostname=tserver0.docker.local \
              --net-alias=tserver0.docker.local \
              --name=${container_name}-tserver0 \
              -p 9997:9997 \
              -e 'SVCLIST=datanode,accumulo-tserver' \
              ${tag} /usr/bin/supervisord -n

echo "start accumulo master"
docker run -d --net=${network_name} \
              --hostname=master.docker.local \
              --net-alias=master.docker.local \
              --name=${container_name}-master \
              -p 9999:9999 -p 50095:50095 \
              -e 'SVCLIST=accumulo-master,accumulo-monitor' \
              ${tag} /usr/bin/supervisord -n

echo "start accumulo proxy, gc and tracer"
docker run -d --net=${network_name} \
              --hostname=proxy.docker.local \
              --net-alias=proxy.docker.local \
              --name=${container_name}-proxy \
              -p 42424:42424 \
              -e "SVCLIST=accumulo-tracer,accumulo-gc,accumulo-proxy" \
              ${tag} /usr/bin/supervisord -n


echo "wait for accumulo services to start"
sleep 10
echo create user bigdawg with password bigdawg
docker exec ${container_name}-tserver0 /tmp/add_user.sh bigdawg bigdawg
echo
master_ip=$(docker inspect --format '{{(index .NetworkSettings.Networks "bigdawg").IPAddress}}' ${container_name}-master)
echo "Accumulo Monitor is at http://master.docker.local:50095"
echo -e "Login to accumulo with \n\t docker exec -u accumulo -it ${container_name}-tserver0 /usr/lib/accumulo/bin/accumulo shell -u bigdawg -p bigdawg"
echo


echo
echo "========================"
echo "===== Loading data ====="
echo "========================"

# postgres-catalog
docker exec -u root bigdawg-postgres-catalog mkdir -p /src/main/resources
docker cp ../src/main/resources/PostgresParserTerms.csv bigdawg-postgres-catalog:/src/main/resources
docker cp ../src/main/resources/SciDBParserTerms.csv bigdawg-postgres-catalog:/src/main/resources
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

# scidb
docker cp cluster_setup/scidb-data/bdsetup bigdawg-scidb-data:/home/scidb/
docker exec bigdawg-scidb-data /home/scidb/bdsetup/setup.sh

# accumulo
docker cp cluster_setup/accumulo-data/bdsetup bigdawg-accumulo-zookeeper:/
docker exec bigdawg-accumulo-zookeeper /bdsetup/insertData.sh

echo
echo "======================================="
echo "===== Starting BigDAWG Middleware ====="
echo "======================================="
docker exec -d bigdawg-scidb-data java -classpath "istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar" istc.bigdawg.Main bigdawg-scidb-data
docker exec bigdawg-postgres-catalog java -classpath "istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar" istc.bigdawg.Main bigdawg-postgres-catalog

echo
echo "================="
echo "===== Done. ====="
echo "================="
