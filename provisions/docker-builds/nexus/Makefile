hostname := nexus

include ../shared/Makefile.vars
include ../shared/Makefile

container:
	sudo docker run -d --name=$(container_name) -p=$(IP):8081:8081 $(tag)
	sleep 10
	sudo docker logs $(container_name)
	echo
	echo "Nexus will be available at http://$(IP):8081"

