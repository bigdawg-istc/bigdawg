hostname := dnsmasq

include ../shared/Makefile.vars
include ../shared/Makefile

container:
	$(DOCKER_CMD) run -d -v=$(CURDIR)/dnsmasq.hosts:/dnsmasq.hosts --name=$(container_name) --publish=$(IP):5353:5353 $(tag)

