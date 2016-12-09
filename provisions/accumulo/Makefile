dnsdomain := node.docker-accumulo.local

EXECUTOR_NUMBER?=0
DOCKER_CMD := docker
# box_name = $(notdir $(CURDIR))
USER := $(shell id -un)
#container_name = $(box_name)-$(USER)-$(EXECUTOR_NUMBER)
#reg := docker.io
#tag := $(reg)/$(box_name)
sshopts := -oStrictHostKeyChecking=no -oUserKnownHostsFile=/dev/null -i $(CURDIR)/../shared/insecure.pem

tag := bigdawg/accumulo
container_name := bigdawg-accumulo

hostname_template := $(container_name).$(dnsdomain)

image:	
	$(DOCKER_CMD) build -t=$(tag) .

cluster:
	$(DOCKER_CMD) run -d --name=consul-leader --hostname=consul.$(dnsdomain) $(tag) /usr/sbin/consul agent -server -bootstrap-expect=1 -data-dir=/var/lib/consul/data -config-dir=/etc/consul-leader
	$(DOCKER_CMD) run -d --dns-search=$(dnsdomain) --hostname=namenode.$(dnsdomain) --dns="$$($(DOCKER_CMD) inspect --format '{{ .NetworkSettings.IPAddress }}' consul-leader)" -e="SVCLIST=namenode,secondarynamenode,datanode" --name=$(container_name) --link=consul-leader:consul-leader $(tag) /usr/bin/supervisord -n
	sleep 1
	$(DOCKER_CMD) run -d --dns-search=$(dnsdomain) --hostname=resourcemanager.$(dnsdomain) --dns="$$($(DOCKER_CMD) inspect --format '{{ .NetworkSettings.IPAddress }}' consul-leader)" -e="SVCLIST=resourcemanager" --name=$(container_name)-rm --link=consul-leader:consul-leader $(tag) /usr/bin/supervisord -n
	$(DOCKER_CMD) run -d --dns-search=$(dnsdomain) --hostname=zookeeper.$(dnsdomain) --dns="$$($(DOCKER_CMD) inspect --format '{{ .NetworkSettings.IPAddress }}' consul-leader)" -e="SVCLIST=zookeeper" --name=$(container_name)-zk0 --link=consul-leader:consul-leader $(tag) /usr/bin/supervisord -n
	sleep 20
	$(DOCKER_CMD) exec $(container_name) /usr/local/sbin/init_accumulo.sh
	$(DOCKER_CMD) run -d --dns-search=$(dnsdomain) --hostname=tserver0.$(dnsdomain) --dns="$$($(DOCKER_CMD) inspect --format '{{ .NetworkSettings.IPAddress }}' consul-leader)" -e="SVCLIST=datanode,nodemanager,accumulo-tserver" --name=$(container_name)-tserver0 --link=consul-leader:consul-leader $(tag) /usr/bin/supervisord -n
	$(DOCKER_CMD) run -d --dns-search=$(dnsdomain) --hostname=tserver1.$(dnsdomain) --dns="$$($(DOCKER_CMD) inspect --format '{{ .NetworkSettings.IPAddress }}' consul-leader)" -e="SVCLIST=datanode,nodemanager,accumulo-tserver" --name=$(container_name)-tserver1 --link=consul-leader:consul-leader $(tag) /usr/bin/supervisord -n
	$(DOCKER_CMD) run -d --dns-search=$(dnsdomain) --hostname=master.$(dnsdomain) --dns="$$($(DOCKER_CMD) inspect --format '{{ .NetworkSettings.IPAddress }}' consul-leader)" -e="SVCLIST=accumulo-master,accumulo-monitor,accumulo-gc,accumulo-tracer" --name=$(container_name)-master --link=consul-leader:consul-leader $(tag) /usr/bin/supervisord -n
	$(DOCKER_CMD) run -d --dns-search=$(dnsdomain) --hostname=proxy.$(dnsdomain) --dns="$$($(DOCKER_CMD) inspect --format '{{ .NetworkSettings.IPAddress }}' consul-leader)" -e="SVCLIST=accumulo-proxy" --name=$(container_name)-proxy --link=consul-leader:consul-leader $(tag) /usr/bin/supervisord -n

add-user:
	$(DOCKER_CMD) exec $(container_name)-tserver0 /usr/local/sbin/add_user.sh

# remove all containers, running or not
clean:
	$(DOCKER_CMD) rm -f consul-leader || :
	$(DOCKER_CMD) rm -f $(container_name)-proxy || :
	$(DOCKER_CMD) rm -f $(container_name)-zk0 || :
	$(DOCKER_CMD) rm -f $(container_name)-rm || :
	$(DOCKER_CMD) rm -f $(container_name)-tserver0 || :
	$(DOCKER_CMD) rm -f $(container_name)-tserver1 || :
	$(DOCKER_CMD) rm -f $(container_name)-master || :
	$(DOCKER_CMD) rm -f $(container_name) || :

erase: clean
	$(DOCKER_CMD) rmi $(tag) 

# enter the consul container
exec-consul:
	$(DOCKER_CMD) exec -i -t consul-leader /bin/bash

exec-nn:
	$(DOCKER_CMD) exec -i -t $(container_name) /bin/bash

exec-tserver0:
	$(DOCKER_CMD) exec -i -t $(container_name)-tserver0 /bin/bash

exec-tserver1:
	$(DOCKER_CMD) exec -i -t $(container_name)-tserver1 /bin/bash

exec-zk0:
	$(DOCKER_CMD) exec -i -t $(container_name)-zk0 /bin/bash

exec-master:
	$(DOCKER_CMD) exec -i -t $(container_name)-master /bin/bash

info:
	@echo "  Consul UI at http://$$($(DOCKER_CMD) inspect --format '{{ .NetworkSettings.IPAddress }}' consul-leader):8500"
	@echo "  HDFS Namenode at http://$$($(DOCKER_CMD) inspect --format '{{ .NetworkSettings.IPAddress }}' $(container_name)):50070"
	@echo "  Accumulo Master at http://$$($(DOCKER_CMD) inspect --format '{{ .NetworkSettings.IPAddress }}' $(container_name)-master):50095"
	@echo "  Tablet servers are at $$($(DOCKER_CMD) inspect --format '{{ .NetworkSettings.IPAddress }}' $(container_name)-tserver0) and $$($(DOCKER_CMD) inspect --format '{{ .NetworkSettings.IPAddress }}' $(container_name)-tserver1)"
	@echo "  Resourcemanager at http://$$($(DOCKER_CMD) inspect --format '{{ .NetworkSettings.IPAddress }}' $(container_name)-rm):8088"
	@echo "  Zookeeper is at $$($(DOCKER_CMD) inspect --format '{{ .NetworkSettings.IPAddress }}' $(container_name)-zk0):2181"

consul-logs:
	$(DOCKER_CMD) logs -f consul-leader

shell:
	$(DOCKER_CMD) run -i -t $(tag) /bin/bash

accumulo-rootshell:
	$(DOCKER_CMD) exec -it $(container_name)-tserver0 su - accumulo -c "/usr/lib/accumulo/bin/accumulo shell -u root -p DOCKERDEFAULT"

accumulo-shell:
	$(DOCKER_CMD) exec -it $(container_name)-tserver0 su - accumulo -c "/usr/lib/accumulo/bin/accumulo shell -u bob -p robert"
