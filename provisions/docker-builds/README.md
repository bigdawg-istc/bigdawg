# Docker Builds

A collection of docker projects for daily use - generally testing. The general idea includes the use of a Makefile for interaction with and automated configuration of images and containers:

* directory name is the box name (from which the image and container names are derived)
* general make commands include
  * _image_: yes - make an image
  * _container_: create a new container
  * _ssh_: log into a running container (if supervisor/sshd are running)
  * _shell_: run a shell container using the current image
  * _logs_
  * _clean_: stop and remove container(s), remove temporary files
  * _erase_: clean + remove the image

Some of the projects have more make commands available. To avoid collisions as good as possible any container will only bind to one specific loopback IP. Worked into IP determination is - among other things - the environment variable EXECUTOR_NUMBER (defaulting to 0) which if set inside a jenkins job allows for parallel execution of the same container without bind conflicts.

## centos-base-ssh

All other projects build on top of this. OpenJDK7, ssh (running on port 2212) on top of CentOS 6.

## zookeeper

Just runs a single zookeeper process, no ssh.

## doop

Fully configured Hortonworks HDP-2.2 (Hadoop 2.6) cluster on CentOS 7. Nodes (containers) are started from one and the same image, with the SVCLIST environment variable controlling which services should run in each container.
Uses a dedicated consul.io container (started first) node discovery, service checks and dns. All (except the consul master) containers use supervisor to startup the consul agent and the listed services.
There is no sshd - all ops interaction uses _docker exec_.

## accumulo

Built on top of doop - provides an easy way to run an Accumulo cluster (currently 1.6.2) in docker.
The choice of node distribution is relatively free as long as there are semantically named hosts (containers) for namenode and zookeeper.
The Makefile is the easiest way to understand the idea of how this works - specifically the _cluster_ target in both - doop and accumulo.

In addition to initialization the `make cluster` command will also create the example user bob with rights to create tables as described in the Accumulo manual.
You can use `make accumulo-shell` and `make accumulo-rootshell` to login.

## nexus

A single Sonatype Nexus java process, no ssh. Good for local maven testing - exposes port 8081.

## zookeeper

A single zookeeper java process, no ssh. Exposes port 2181.

## consul-cluster

Consul by HashiCorp! Try it in single-container or cluster mode.

## firefox

You can run the firefox in this container via X11 forwarding - it runs on top of an 1.6 OpenJDK to be used with older Applets (like in Supermicro server IPMI web console)
