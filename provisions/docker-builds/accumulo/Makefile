dnsdomain := docker.local
-include ../shared/Makefile.vars
-include ../shared/Makefile
NET       := accumulo-local
CIDR      := 172.26.2.0/24
INSPECT_ADDRESS := $(DOCKER_CMD) inspect --format '{{(index .NetworkSettings.Networks "$(NET)").IPAddress}}'

all:
	echo "Can do 'clean', 'image' or 'container'"

image:
	$(DOCKER_CMD) build -t=$(tag) .

cluster: check-network
	@echo Starting hdfs and zookeeper
	@$(DOCKER_CMD) run -d --net=$(NET) --hostname=namenode.$(dnsdomain) --net-alias=namenode.$(dnsdomain) -e="SVCLIST=namenode,datanode" --name=$(container_name) $(tag) /usr/bin/supervisord -n
	@$(DOCKER_CMD) run -d --net=$(NET) --hostname=zookeeper.$(dnsdomain) --net-alias=zookeeper.$(dnsdomain) -e="SVCLIST=zookeeper" --name=$(container_name)-zk0 $(tag) /usr/bin/supervisord -n
	@echo "Wait for hadoop to start"
	@sleep 15
	@echo "Initialize accumulo"
	@$(DOCKER_CMD) exec $(container_name) /usr/lib/accumulo/bin/init_accumulo.sh
	@$(DOCKER_CMD) run -d --net=$(NET) --hostname=master.$(dnsdomain) --net-alias=master.$(dnsdomain) -e="SVCLIST=accumulo-master,accumulo-monitor" --name=$(container_name)-master $(tag) /usr/bin/supervisord -n
	@$(DOCKER_CMD) run -d --net=$(NET) --hostname=tserver0.$(dnsdomain) --net-alias=tserver0.$(dnsdomain) -e="SVCLIST=datanode,accumulo-tserver" --name=$(container_name)-tserver0 $(tag) /usr/bin/supervisord -n
	@$(DOCKER_CMD) run -d --net=$(NET) --hostname=tserver1.$(dnsdomain) --net-alias=tserver1.$(dnsdomain) -e="SVCLIST=datanode,accumulo-tserver" --name=$(container_name)-tserver1 $(tag) /usr/bin/supervisord -n
	@$(DOCKER_CMD) run -d --net=$(NET) --hostname=proxy.$(dnsdomain) --net-alias=proxy.$(dnsdomain) -e="SVCLIST=accumulo-proxy,accumulo-tracer,accumulo--gc" --name=$(container_name)-proxy $(tag) /usr/bin/supervisord -n
	@echo "Wait for accumulo to start"
	@sleep 10
	@echo "Add a user"
	@$(DOCKER_CMD) exec $(container_name)-tserver0 /tmp/add_user.sh
	@echo "The Accumulo monitor page is here: http://$$($(INSPECT_ADDRESS) $(container_name)-master):50095"

clean:
	@$(DOCKER_CMD) rm -f $(container_name)-proxy || :
	@$(DOCKER_CMD) rm -f $(container_name)-zk0 || :
	@$(DOCKER_CMD) rm -f $(container_name)-tserver0 || :
	@$(DOCKER_CMD) rm -f $(container_name)-tserver1 || :
	@$(DOCKER_CMD) rm -f $(container_name)-master || :
	@$(DOCKER_CMD) rm -f $(container_name) || :

accumulo-rootshell:
	$(DOCKER_CMD) exec -it $(container_name)-tserver0 su - accumulo -c "/usr/lib/accumulo/bin/accumulo shell -u root -p DOCKERDEFAULT"

accumulo-shell:
	$(DOCKER_CMD) exec -it $(container_name)-tserver0 su - accumulo -c "/usr/lib/accumulo/bin/accumulo shell -u bob -p robert"

