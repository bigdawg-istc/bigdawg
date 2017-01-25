consul cluster in docker containers

See http://www.consul.io/ for what consul is.
This is just to learn about what can and what cannot be achieved 
with consul (inside docker) - the idea is to use the service registry
and dns to orchestrate services in multiple docker containers from within, without the need for external (and static) name serice.

You can point a browser to port 8500 of any of the nodes for the consul web ui. Also runs an sshd process on port 2211 of each container.

Containers:

- server1: 127.0.0.20
- server2: 127.0.0.21
- server3: 127.0.0.22
- agent1: 127.0.0.23
- agent2: 127.0.0.24
- agent3: 127.0.0.25

Use the Makefile (`make image`) to build, (`make cluster`) to run, stop with `make clean`. There is also `make server1` and `make agent1` which will login to the respective container with ssh. Just take a look at a "cold" container with `make shell`.

