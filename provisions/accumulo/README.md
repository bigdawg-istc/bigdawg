# docker-accumulo

This is a WIP project to make accumulo clusters easily available on the docker platform.
It is geared towards development and experimentation and currently lacks attributes of production-readiness
as true persistence or access control. DO NOT USE IN PRODUCTION!
The workflow is currently arranged around a Makefile that hides some of the operational complexity.

## Build

Make sure you have docker installed and a daemon running and available.
Simply execute

    make image

A `docker-accumulo` image will be built, containing Hortonworks HDP-2.2 and Accumulo 1.7.0 on CentOS 7.
Additionally there is a consul cluster for DNS and service checks.

## Run

After the image has been created a cluster can be run with

    make cluster

Once the cluster has formed (there is a 20 second sleep command in there to give Hadoop time to start) you can use 

    make info

to get a list of all nodes (their IPs and URLs)
Example:

    > $ make info                                                                                                                                                                      
      Consul UI at http://172.17.0.11:8500
      HDFS Namenode at http://172.17.0.12:50070
      Accumulo Master at http://172.17.0.17:50095
      Tablet servers are at 172.17.0.15 and 172.17.0.16
      Resourcemanager at http://172.17.0.13:8088
      Zookeeper is at 172.17.0.14:2181

If you no longer need your cluster (or want to start over), run

    make clean

This will remove all containers (and all data in them) permanently. If you also want to remove the docker image, use

    make erase

## Use

Optionally on your running cluster you can add a simple test user

    > $ make add-user                                                                                                                                                                  
    ...
    2015-06-09 19:18:43,896 [Shell.audit] INFO : root@accumulo> createuser bob
      Enter new password for 'bob': ******
      Please confirm new password for 'bob': ******
      Enter current password for 'bob': ******
      Valid
    ...
    2015-06-09 19:18:48,471 [Shell.audit] INFO : root@accumulo> grant System.CREATE_TABLE -s -u bob

And after that use

    make accumulo-shell

or

    make accumulo-rootshell

In case you need to work in a regular shell on one of the cluster members, there are `docker exec` shortcuts in the Makefile:

    make exec-consul
    make exec-master
    make exec-nn
    make exec-zk0
    make exec-tserver0
    make exec-tserver1

## Misc

Having a running cluster you can - with the help of consul - find out where your services are using dns queries.
You can find the service names in the consul UI.

![Consul UI](https://raw.githubusercontent.com/accumulo/docker-accumulo/master/accumulo_cluster_in_consul.png)

Example 1: Where is zookeeper again?

    > $ dig @172.17.0.14 zookeeper.service.docker-accumulo.local                                                                                                                       

    ; <<>> DiG 9.9.6-P1-RedHat-9.9.6-8.P1.fc21 <<>> @172.17.0.14 zookeeper.service.docker-accumulo.local
    ; (1 server found)
    ;; global options: +cmd
    ;; Got answer:
    ;; ->>HEADER<<- opcode: QUERY, status: NOERROR, id: 20249
    ;; flags: qr aa rd ra; QUERY: 1, ANSWER: 1, AUTHORITY: 0, ADDITIONAL: 0

    ;; QUESTION SECTION:
    ;zookeeper.service.docker-accumulo.local. IN A

    ;; ANSWER SECTION:
    zookeeper.service.docker-accumulo.local. 0 IN A 172.17.0.14

    ;; Query time: 1 msec
    ;; SERVER: 172.17.0.14#53(172.17.0.14)
    ;; WHEN: Tue Jun 09 15:06:26 EDT 2015
    ;; MSG SIZE  rcvd: 112

As you might expect, the zookeeper address is reported back - but this will only work as long as the service check in consul actually "sees" zookeeper to be online.

Example 2: Where are my tablet servers?

    > $ dig @172.17.0.14 accumulo-tserver.service.docker-accumulo.local                                                                                                                

    ; <<>> DiG 9.9.6-P1-RedHat-9.9.6-8.P1.fc21 <<>> @172.17.0.14 accumulo-tserver.service.docker-accumulo.local
    ; (1 server found)
    ;; global options: +cmd
    ;; Got answer:
    ;; ->>HEADER<<- opcode: QUERY, status: NOERROR, id: 51261
    ;; flags: qr aa rd ra; QUERY: 1, ANSWER: 2, AUTHORITY: 0, ADDITIONAL: 0
    
    ;; QUESTION SECTION:
    ;accumulo-tserver.service.docker-accumulo.local.  IN A
    
    ;; ANSWER SECTION:
    accumulo-tserver.service.docker-accumulo.local. 0 IN A 172.17.0.16
    accumulo-tserver.service.docker-accumulo.local. 0 IN A 172.17.0.15
    
    ;; Query time: 1 msec
    ;; SERVER: 172.17.0.14#53(172.17.0.14)
    ;; WHEN: Tue Jun 09 15:01:25 EDT 2015
    ;; MSG SIZE  rcvd: 188

You can see that both addresses are reported because we have two service instances registered with consul.
Were zookeeper configured as a cluster with 3 containers the first examples would have shown all three of them respectively.

## TODO

- use consul-template for configuration
- test multi-host deployments with swarm or others

