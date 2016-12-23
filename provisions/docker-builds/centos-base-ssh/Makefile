hostname := centos-base

include ../shared/Makefile.vars
tag := "$(reg)/$(box_name):7"
include ../shared/Makefile

image:
	cp ../shared/insecure.pub $(CURDIR)/
	$(DOCKER_CMD) build -t=$(tag) .

