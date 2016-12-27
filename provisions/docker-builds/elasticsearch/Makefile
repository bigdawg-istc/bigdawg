include ../shared/Makefile.vars
include ../shared/Makefile

container:
	$(DOCKER_CMD) run -d --name=$(container_name) $(tag)
	echo For the UI go to http://$$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' $(container_name)):9200/_plugin/head
	echo iMarvel is at http://$$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' $(container_name)):9200/_plugin/marvel

node1:
	$(DOCKER_CMD) run -d --name=$(container_name)-1 $(tag)

node2:
	$(DOCKER_CMD) run -d --name=$(container_name)-2 $(tag)

cluster: container node1 node2

clean:
	$(DOCKER_CMD) rm -f $(container_name) 2>/dev/null || :
	$(DOCKER_CMD) rm -f $(container_name)-1 2>/dev/null || :
	$(DOCKER_CMD) rm -f $(container_name)-2 2>/dev/null || :

shell:
	$(DOCKER_CMD) run -t -i --rm --name=$(container_name)-shell $(tag) /usr/bin/bash
