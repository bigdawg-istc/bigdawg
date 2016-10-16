# Ubuntu 15.04 with Java 8 installed.
# Build image with: docker build -t bigdawg/ubuntu1504java8 .
# https://dzone.com/articles/creating-a-docker-image-with-ubuntu-and-java
FROM ubuntu:15.04

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y  software-properties-common && \
    add-apt-repository ppa:webupd8team/java -y && \
    apt-get update && \
    echo oracle-java7-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections && \
    apt-get install -y oracle-java8-installer && \
    apt-get clean

