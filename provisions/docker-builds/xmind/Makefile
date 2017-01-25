include ../shared/Makefile.vars
tag := "$(reg)/$(box_name)"
include ../shared/Makefile

xmind:
	docker run -it --net host --cpuset-cpus 0 --memory 2048mb \
		     -v /tmp/.X11-unix:/tmp/.X11-unix \
		     -v $(HOME)/Downloads:/root/Downloads -v $(HOME)/Documents:/root/Documents \
		     --device /dev/snd --name $(container_name) $(tag)

xshell:
	docker run -it --net host --cpuset-cpus 0 --memory 2048mb -v /tmp/.X11-unix:/tmp/.X11-unix -v $(HOME)/Downloads:/root/Downloads -v $(HOME)/Documents:/root/Documents --device /dev/snd --name $(container_name) $(tag) /usr/bin/xterm
