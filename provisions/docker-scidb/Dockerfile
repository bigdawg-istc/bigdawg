# SciDB 14.12
#
# VERSION 1.0
#
#
#
#
#
#
#PORT MAPPING
#SERVICE		DEFAULT
#ssh 			22
#shim			8083s
#Postgresql 		5432
#SciDB			1239		


FROM ubuntu:12.04
MAINTAINER Alber Sanchez



# install
RUN apt-get -qq update && apt-get install --fix-missing -y --force-yes --allow-unauthenticated \
	openssh-server \
	sudo \
	wget \
	gdebi \
	nano \  
	postgresql-8.4 \ 
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
RUN useradd --home /home/scidb --create-home --uid 1005 --group sudo --shell /bin/bash scidb
RUN echo 'root:xxxx.xxxx.xxxx' | chpasswd
RUN echo 'postgres:xxxx.xxxx.xxxx' | chpasswd
RUN echo 'scidb:xxxx.xxxx.xxxx' | chpasswd
RUN echo 'xxxx.xxxx.xxxx'  >> /home/scidb/pass.txt


RUN mkdir /var/run/sshd
RUN mkdir /home/scidb/data
RUN mkdir /home/scidb/catalog


# Configure SSH
RUN echo 'StrictHostKeyChecking no' >> /etc/ssh/ssh_config


# Configure Postgres 
RUN echo 'host  all all 255.255.0.0/16   md5' >> /etc/postgresql/8.4/main/pg_hba.conf


# Add files
ADD containerSetup.sh /root/containerSetup.sh
ADD conf /root/conf
ADD iquery.conf /home/scidb/.config/scidb/iquery.conf
ADD installPackages.R /home/scidb/installPackages.R
ADD startScidb.sh /home/scidb/startScidb.sh
ADD stopScidb.sh /home/scidb/stopScidb.sh
ADD scidb_docker.ini /home/scidb/scidb_docker.ini


RUN chown -R root:root /root/*


RUN chown -R scidb:scidb /home/scidb/*


RUN chmod +x \ 
	/root/*.sh \ 
	/home/scidb/*.sh 


# Restarting services
RUN stop ssh
RUN start ssh
RUN /etc/init.d/postgresql restart

	
EXPOSE 22
EXPOSE 8083

# Added by kyle:
ENV USER scidb
ADD initScidb.sh /
RUN /usr/sbin/sshd && /root/containerSetup.sh
CMD /initScidb.sh

#CMD    ["/usr/sbin/sshd", "-D"]
