# COMPILE SciDB 15.12
#
# VERSION 1.0
#
#
#
#
#
#PORTS
#ssh					22
#Postgresql		5432
#SciDB 				1239

FROM ubuntu:12.04
MAINTAINER Alber Sanchez


# install
RUN apt-get -qq update && apt-get install --fix-missing -y --force-yes \
	apt-transport-https \
	apt-utils \
	curl \
	dialog \
	expect \
	gcc \
	git \
	libc-dev-bin \
	libc6-dbg \
	libc6-dev \
	libcurl3-dev \
	libgomp1 \
	libssl-dev \
	linux-libc-dev \
	nano \
	openssh-server \
	openssh-client \
	postgresql-8.4 \
	subversion \
	sudo \
	ssh \
	sshpass \
	wget \
	zlib1g-dev


# Set environment
RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8
RUN env


# Configure users
RUN useradd --home /home/scidb --create-home --uid 1005 --group sudo --shell /bin/bash scidb
RUN usermod -u 1004 -U scidb
RUN groupmod -g 1004 scidb
RUN usermod -a -G sudo scidb
RUN echo 'root:xxxx.xxxx.xxxx' | chpasswd
RUN echo 'postgres:xxxx.xxxx.xxxx' | chpasswd
RUN echo 'scidb:xxxx.xxxx.xxxx' | chpasswd
RUN mkdir /home/scidb/data
RUN chown scidb:scidb /home/scidb/data


# Configure SSH
RUN mkdir /var/run/sshd
RUN echo 'StrictHostKeyChecking no' >> /etc/ssh/ssh_config


# Configure Postgres
RUN echo 'host all all 255.255.0.0/16 md5' >> /etc/postgresql/8.4/main/pg_hba.conf


#Add required files
ADD containerSetup.sh 						/root/containerSetup.sh
ADD installR.sh 									/root/installR.sh
ADD scidb_docker.ini 							/home/scidb/scidb_docker.ini
ADD scidb-15.12.1.4cadab5.tar.gz	/home/scidb/dev_dir
ADD installPackages.R							/home/scidb/installPackages.R


RUN chown -R scidb:scidb /home/scidb/dev_dir
RUN chown root:root  /root/*.*
RUN chmod +x /root/*.sh


# Restarting services
RUN stop ssh
RUN start ssh


EXPOSE 22
EXPOSE 1239
EXPOSE 5432


CMD    ["/usr/sbin/sshd", "-D"]
