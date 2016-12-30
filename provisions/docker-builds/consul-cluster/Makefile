hostname := namenode

include ../shared/Makefile.vars
include ../shared/Makefile

data_dir := /var/lib/consul/data
ui_dir := /var/lib/consul/ui/dist

clean:
	sudo docker rm -f $(container_name) || :
	sudo docker rm -f server1 || :
	sudo docker rm -f server2 || :
	sudo docker rm -f server3 || :
	sudo docker rm -f server4 || :
	sudo docker rm -f agent1 || :
	sudo docker rm -f agent2 || :
	sudo docker rm -f agent3 || :

erase:	clean
	sudo docker rmi $(tag)

cluster:
	sudo docker run -d --env='SRV_ARGS=-server -bootstrap' --name=server1 --hostname=server1 -p=127.0.0.20:2212:2212 -p=127.0.0.20:8600:53 -p=127.0.0.20:8500:8500 $(tag) /usr/bin/supervisord -n
	sudo docker run -d --link=server1:peer --env='SRV_ARGS=-server -join=peer' --name=server2 --hostname=server2 -p=127.0.0.21:2212:2212 -p=127.0.0.21:8500:8500 $(tag) /usr/bin/supervisord -n
	sudo docker run -d --link=server2:peer --env='SRV_ARGS=-server -join=peer' --name=server3 --hostname=server3 -p=127.0.0.22:2212:2212 -p=127.0.0.22:8500:8500 $(tag) /usr/bin/supervisord -n
	sudo docker run -d --link=server3:peer --env='SRV_ARGS=-join=peer' --name=agent1 --hostname=agent1 -p=127.0.0.23:2212:2212 -p=127.0.0.23:8500:8500 $(tag) /usr/bin/supervisord -n
	sudo docker run -d --link=agent1:peer --env='SRV_ARGS=-join=peer' --name=agent2 --hostname=agent2 -p=127.0.0.24:2212:2212 -p=127.0.0.24:8500:8500 $(tag) /usr/bin/supervisord -n
	sudo docker run -d --link=agent2:peer --env='SRV_ARGS=-join=peer' --name=agent3 --hostname=agent3 -p=127.0.0.25:2212:2212 -p=127.0.0.25:8500:8500 $(tag) /usr/bin/supervisord -n

cluster-noquorum2:
	sudo docker run -d --dns=localhost --env='SRV_ARGS=-server -bootstrap' --name=server1 --hostname=server1 -p=127.0.0.20:2212:2212 -p=127.0.0.20:8600:53 -p=127.0.0.20:8500:8500 $(tag) /usr/bin/supervisord -n
	sudo docker run -d --link=server1:peer --env='SRV_ARGS=-server -join=peer' --name=server2 --hostname=server2 -p=127.0.0.21:2212:2212 -p=127.0.0.21:8500:8500 $(tag) /usr/bin/supervisord -n
	sudo docker run -d --link=server1:peer --env='SRV_ARGS=-join=peer' --name=agent2 --hostname=agent2 -p=127.0.0.24:2212:2212 -p=127.0.0.24:8500:8500 $(tag) /usr/bin/supervisord -n
	sudo docker run -d --link=agent2:peer --env='SRV_ARGS=-join=peer' --name=agent3 --hostname=agent3 -p=127.0.0.25:2212:2212 -p=127.0.0.25:8500:8500 $(tag) /usr/bin/supervisord -n

cluster-noquorum4:
	sudo docker run -d --dns=localhost --env='SRV_ARGS=-server -bootstrap' --name=server1 --hostname=server1 -p=127.0.0.20:2212:2212 -p=127.0.0.20:8600:53 -p=127.0.0.20:8500:8500 $(tag) /usr/bin/supervisord -n
	sudo docker run -d --link=server1:peer --env='SRV_ARGS=-server -join=peer' --name=server2 --hostname=server2 -p=127.0.0.21:2212:2212 -p=127.0.0.21:8500:8500 $(tag) /usr/bin/supervisord -n
	sudo docker run -d --link=server2:peer --env='SRV_ARGS=-server -join=peer' --name=server3 --hostname=server3 -p=127.0.0.22:2212:2212 -p=127.0.0.22:8500:8500 $(tag) /usr/bin/supervisord -n
	sudo docker run -d --link=server3:peer --env='SRV_ARGS=-server -join=peer' --name=server4 --hostname=server4 -p=127.0.0.26:2212:2212 -p=127.0.0.26:8500:8500 $(tag) /usr/bin/supervisord -n
	sudo docker run -d --link=server3:peer --env='SRV_ARGS=-join=peer' --name=agent1 --hostname=agent1 -p=127.0.0.23:2212:2212 -p=127.0.0.23:8500:8500 $(tag) /usr/bin/supervisord -n
	sudo docker run -d --link=agent1:peer --env='SRV_ARGS=-join=peer' --name=agent2 --hostname=agent2 -p=127.0.0.24:2212:2212 -p=127.0.0.24:8500:8500 $(tag) /usr/bin/supervisord -n

server1:
	ssh $(sshopts) root@$$($(DOCKER_CMD) inspect --format '{{ .NetworkSettings.IPAddress }}' server1)

agent1:
	ssh $(sshopts) root@$$($(DOCKER_CMD) inspect --format '{{ .NetworkSettings.IPAddress }}' agent1)
