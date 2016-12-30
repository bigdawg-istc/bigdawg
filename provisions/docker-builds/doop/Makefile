dnsdomain := docker.local
NET       := doop-local
CIDR      := 172.26.1.0/24

include ../shared/Makefile.vars
hostname_template := $(container_name).$(dnsdomain)
include ../shared/Makefile
INSPECT_ALIAS := $(DOCKER_CMD) inspect --format '{{(index .NetworkSettings.Networks "$(NET)").Aliases}}'
INSPECT_ADDRESS := $(DOCKER_CMD) inspect --format '{{(index .NetworkSettings.Networks "$(NET)").IPAddress}}'

image:	
	cp -rv ../shared/config.zk $(CURDIR)/conf.zk
	$(DOCKER_CMD) build -t=$(tag) .

cluster: check-network
	$(DOCKER_CMD) run -d --net=$(NET) --hostname=namenode.$(dnsdomain) --net-alias=namenode.$(dnsdomain) -e="SVCLIST=namenode,secondarynamenode" --name=$(container_name) $(tag) /usr/bin/supervisord -n
	$(DOCKER_CMD) run -d --net=$(NET) --hostname=resourcemanager.$(dnsdomain) --net-alias=resourcemanager.$(dnsdomain) -e="SVCLIST=resourcemanager" --name=$(container_name)-rm $(tag) /usr/bin/supervisord -n
	$(DOCKER_CMD) run -d --net=$(NET) --hostname=datanode0.$(dnsdomain) --net-alias=datanode0.$(dnsdomain) -e="SVCLIST=datanode,nodemanager" --name=$(container_name)-dn0 $(tag) /usr/bin/supervisord -n
	$(DOCKER_CMD) run -d --net=$(NET) --hostname=datanode1.$(dnsdomain) --net-alias=datanode1.$(dnsdomain) -e="SVCLIST=datanode,nodemanager" --name=$(container_name)-dn1 $(tag) /usr/bin/supervisord -n
	$(DOCKER_CMD) run -d --net=$(NET) --hostname=zookeeper.$(dnsdomain) --net-alias=zookeeper.$(dnsdomain) -e="SVCLIST=zookeeper" --name=$(container_name)-zk0 $(tag) /usr/bin/supervisord -n

clean:
	@$(DOCKER_CMD) rm -f $(container_name)-zk0 || :
	@$(DOCKER_CMD) rm -f $(container_name)-rm || :
	@$(DOCKER_CMD) rm -f $(container_name)-dn0 || :
	@$(DOCKER_CMD) rm -f $(container_name)-dn1 || :
	@$(DOCKER_CMD) rm -f $(container_name) || :

exec-nn:
	$(DOCKER_CMD) exec -i -t $(container_name) /bin/bash

exec-rm:
	$(DOCKER_CMD) exec -i -t $(container_name)-rm /bin/bash

exec-dn0:
	$(DOCKER_CMD) exec -i -t $(container_name)-dn0 /bin/bash

exec-dn1:
	$(DOCKER_CMD) exec -i -t $(container_name)-dn1 /bin/bash

info:
	@$(INSPECT_ALIAS) $(container_name)
	@$(INSPECT_ADDRESS) $(container_name)
	@$(INSPECT_ALIAS) $(container_name)-dn0
	@$(INSPECT_ADDRESS) $(container_name)-dn0
	@$(INSPECT_ALIAS) $(container_name)-dn1
	@$(INSPECT_ADDRESS) $(container_name)-dn1
	@$(INSPECT_ALIAS) $(container_name)-rm
	@$(INSPECT_ADDRESS) $(container_name)-rm
	@$(INSPECT_ALIAS) $(container_name)-zk0
	@$(INSPECT_ADDRESS) $(container_name)-zk0

nn-logs:
	$(DOCKER_CMD) exec -i -t $(container_name) tail -40f /var/log/hadoop/hdfs-namenode.log

