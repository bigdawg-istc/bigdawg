# https://github.com/albhasan/docker_scidb
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

RUN apt-get -qq update && \
	apt-get install --fix-missing -y --force-yes --allow-unauthenticated \
	openssh-server \
	postgresql-8.4 \
	sudo \
	wget \
	gdebi \
	nano \
	sshpass \
	git-core \
	apt-transport-https \
	net-tools


# Set environment
RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8
RUN env


# Configure users
RUN useradd --home /home/scidb --create-home --uid 1005 --group sudo --shell /bin/bash scidb && \
	echo 'root:xxxx.xxxx.xxxx' | chpasswd && \
	echo 'postgres:xxxx.xxxx.xxxx' | chpasswd && \
	echo 'scidb:xxxx.xxxx.xxxx' | chpasswd && \
	echo 'xxxx.xxxx.xxxx'  >> /home/scidb/pass.txt

# Configure SSH
RUN echo 'StrictHostKeyChecking no' >> /etc/ssh/ssh_config

# Configure Postgres 
RUN echo 'host  all all 255.255.0.0/16   md5' >> /etc/postgresql/8.4/main/pg_hba.conf

# Add supporting files
ADD image_contents/containerSetup.sh /root/containerSetup.sh
ADD image_contents/conf /root/conf
ADD image_contents/iquery.conf /home/scidb/.config/scidb/iquery.conf
ADD image_contents/installPackages.R /home/scidb/installPackages.R
ADD image_contents/startScidb.sh /home/scidb/startScidb.sh
ADD image_contents/stopScidb.sh /home/scidb/stopScidb.sh
ADD image_contents/scidb_docker.ini /home/scidb/scidb_docker.ini


# Run the scidb installer
ADD image_contents/deployment-14.12.tar /home/scidb
RUN mkdir /var/run/sshd /home/scidb/data /home/scidb/catalog && \
	/usr/sbin/sshd && \
	/etc/init.d/postgresql start && \
	export USER=root && \
	cd /home/scidb/deployment/cluster_install && \
	yes | ./cluster_install -s -u scidb /home/scidb/scidb_docker.ini 

RUN chown -R root:root /root/* && \
	chown -R scidb:scidb /home/scidb/* && \
	chmod +x /root/*.sh /home/scidb/*.sh 

EXPOSE 22
EXPOSE 8083

COPY image_contents/start_services.sh /
CMD /start_services.sh
