#!/bin/bash

# Note about hostnames: Container hostnames must match those configured in "accumulo.conf/slaves" and "accumulo.conf/masters"

tag=bigdawg/accumulo-data
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

#echo "start tserver1"
#docker run -d --net=${network_name} \
#              --hostname=tserver1.docker.local \
#              --net-alias=tserver1.docker.local \
#              --name=${container_name}-tserver1 \
#              --expose=9997 \
#              -e 'SVCLIST=datanode,accumulo-tserver' \
#              ${tag} /usr/bin/supervisord -n

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
