hostname := centos-base

include ../shared/Makefile.vars
include ../shared/Makefile

image:
	cp -r ../shared/config.supervisor conf.supervisor
	cp -r ../shared/config.zk conf.zk
	$(DOCKER_CMD) build -t=$(tag) .

container:
	$(DOCKER_CMD) run -d --name=$(container_name) --publish=$(IP):2181:2181 $(tag)

