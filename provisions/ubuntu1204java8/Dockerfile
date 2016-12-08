# Ubuntu 12.04 with Java 8 installed
# https://dzone.com/articles/creating-a-docker-image-with-ubuntu-and-java

FROM ubuntu:12.04

RUN apt-get update && \
    apt-get dist-upgrade -y && \
    apt-get install -y software-properties-common && \
    apt-get install -y python-software-properties && \
    add-apt-repository ppa:webupd8team/java -y && \
    apt-get update && \
    echo oracle-java7-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections && \
    apt-get install -y oracle-java8-installer && \
    apt-get install -y netcat && \
	apt-get install -y curl && \
	apt-get install -y net-tools && \
	apt-get install -y iputils-ping && \
    apt-get clean

ADD istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar /