#!/bin/bash

tag=bigdawg/accumulo-data
container_name=accumulo-data
accumulo_net=bigdawg

if [ "$1" = "-clean" -o "$1" = "-shutdown" ]
then
  echo "Shutting down all Accumulo containers ($1)"
  for c in namenode zookeeper tserver0 tserver1 master proxy
  do
		n=${container_name}-${c}
		docker inspect $n &>/dev/null && docker rm -f $n
  done
fi

if [ "$1" = "-shutdown" ]
then
  echo "Thats it"
  exit 0
fi

# docker pull $tag

run_container() {
  fqdn="${1}.docker.local"
  docker run -d --net=${accumulo_net} \
                         --hostname=${fqdn} --net-alias=${fqdn} \
                         -e="SVCLIST=${2}" \
                         --name=${container_name}-${1} ${tag} \
                         /usr/bin/supervisord -n
}

# the cluster requires DNS to work which we use a custom docker network
# with its embedded DNS server for
$(docker network inspect ${accumulo_net} &>/dev/null) || {
  # TODO: would be nice to check that the local docker client is new enough for this
  docker network create -d bridge --subnet=172.25.10.0/24 $accumulo_net
}

echo "Welcome to Accumulo on Docker!"
echo

echo "starting hdfs"
run_container namenode 'namenode,datanode'

echo "start zookeeper"
docker run -d --net=${accumulo_net} \
                         --hostname=zookeeper.docker.local --net-alias=zookeeper.docker.local \
                         -p 2181:2181 \
                         -e='SVCLIST=zookeeper' \
                         --name=accumulo-data-zookeeper ${tag} \
                         /usr/bin/supervisord -n

echo "hdfs needs a moment to become available"
sleep 10

echo "initializing Accumulo instance"
docker exec ${container_name}-namenode /usr/lib/accumulo/bin/init_accumulo.sh

echo "start tservers"
docker run -d --net=${accumulo_net} \
                         --hostname=tserver0.docker.local --net-alias=tserver0.docker.local \
                         -p 9997:9997 \
                         -e='SVCLIST=datanode,accumulo-tserver' \
                         --name=accumulo-data-tserver0 ${tag} \
                         /usr/bin/supervisord -n
run_container tserver1 'datanode,accumulo-tserver'

echo "start accumulo master"
docker run -d --net=${accumulo_net} \
                         --hostname=master.docker.local --net-alias=master.docker.local \
                         -p 9999:9999 -p 50095:50095 \
                         -e='SVCLIST=accumulo-master,accumulo-monitor' \
                         --name=accumulo-data-master ${tag} \
                         /image_contents/start_services.sh

echo "start accumulo proxy, gc and tracer"
run_container proxy 'accumulo-tracer,accumulo-gc,accumulo-proxy'
echo "wait for accumulo services to start"
sleep 10
echo create user bigdawg with password bigdawg
docker exec ${container_name}-tserver0 /tmp/add_user.sh bigdawg bigdawg
echo

master_ip=$(docker inspect --format '{{(index .NetworkSettings.Networks "bigdawg").IPAddress}}' ${container_name}-master)
echo "Accumulo Monitor is at http://${master_ip}:50095"
echo -e "Login to accumulo with \n\t docker exec -u accumulo -it ${container_name}-tserver0 /usr/lib/accumulo/bin/accumulo shell -u bigdawg -p bigdawg"
echo
