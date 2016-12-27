dnsdomain := docker.local

include ../shared/Makefile.vars
hostname_template := $(container_name).$(dnsdomain)
include ../shared/Makefile

image:	
	cp -rv ../shared/config.zk $(CURDIR)/conf.zk
	$(DOCKER_CMD) build -t=$(tag) .

cluster:
	$(DOCKER_CMD) run -d --dns-search=node.doop.local --name=consul-leader $(tag) /usr/sbin/consul agent -server -bootstrap-expect=1 -data-dir=/var/lib/consul/data -config-dir=/etc/consul-leader
	$(DOCKER_CMD) run -d --dns-search=node.doop.local --hostname=namenode --dns="$$($(DOCKER_CMD) inspect --format '{{ .NetworkSettings.IPAddress }}' consul-leader)" -e="SVCLIST=namenode,secondarynamenode" --name=$(container_name) --link=consul-leader:consul-leader $(tag) /usr/bin/supervisord -n
	sleep 1
	$(DOCKER_CMD) run -d --dns-search=node.doop.local --hostname=resourcemanager --dns="$$($(DOCKER_CMD) inspect --format '{{ .NetworkSettings.IPAddress }}' consul-leader)" -e="SVCLIST=resourcemanager" --name=$(container_name)-rm --link=consul-leader:consul-leader $(tag) /usr/bin/supervisord -n
	sleep 1
	$(DOCKER_CMD) run -d --dns-search=node.doop.local --hostname=datanode0 --dns="$$($(DOCKER_CMD) inspect --format '{{ .NetworkSettings.IPAddress }}' consul-leader)" -e="SVCLIST=datanode,nodemanager" --name=$(container_name)-dn0 --link=consul-leader:consul-leader $(tag) /usr/bin/supervisord -n
	$(DOCKER_CMD) run -d --dns-search=node.doop.local --hostname=datanode1 --dns="$$($(DOCKER_CMD) inspect --format '{{ .NetworkSettings.IPAddress }}' consul-leader)" -e="SVCLIST=datanode,nodemanager" --name=$(container_name)-dn1 --link=consul-leader:consul-leader $(tag) /usr/bin/supervisord -n
	$(DOCKER_CMD) run -d --dns-search=node.doop.local --hostname=zookeeper --dns="$$($(DOCKER_CMD) inspect --format '{{ .NetworkSettings.IPAddress }}' consul-leader)" -e="SVCLIST=zookeeper" --name=$(container_name)-zk0 --link=consul-leader:consul-leader $(tag) /usr/bin/supervisord -n

clean:
	$(DOCKER_CMD) rm -f consul-leader || :
	$(DOCKER_CMD) rm -f $(container_name)-zk0 || :
	$(DOCKER_CMD) rm -f $(container_name)-rm || :
	$(DOCKER_CMD) rm -f $(container_name)-dn0 || :
	$(DOCKER_CMD) rm -f $(container_name)-dn1 || :
	$(DOCKER_CMD) rm -f $(container_name) || :

exec-consul:
	$(DOCKER_CMD) exec -i -t consul-leader /bin/bash

exec-dn0:
	$(DOCKER_CMD) exec -i -t $(container_name)-dn0 /bin/bash

exec-dn1:
	$(DOCKER_CMD) exec -i -t $(container_name)-dn1 /bin/bash

info:
	$(DOCKER_CMD) inspect --format '{{ .NetworkSettings.IPAddress }}' consul-leader
	$(DOCKER_CMD) inspect --format '{{ .NetworkSettings.IPAddress }}' $(container_name)
	$(DOCKER_CMD) inspect --format '{{ .NetworkSettings.IPAddress }}' $(container_name)-dn0
	$(DOCKER_CMD) inspect --format '{{ .NetworkSettings.IPAddress }}' $(container_name)-dn1
	$(DOCKER_CMD) inspect --format '{{ .NetworkSettings.IPAddress }}' $(container_name)-rm
	$(DOCKER_CMD) inspect --format '{{ .NetworkSettings.IPAddress }}' $(container_name)-zk0

consul-logs:
	$(DOCKER_CMD) logs -f consul-leader

