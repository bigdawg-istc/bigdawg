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


#FROM ubuntu:12.04

FROM bigdawg/ubuntu1204java8


# Scidb-related:
RUN apt-get install -y openssh-server && \
    apt-get install -y sudo && \
    apt-get install -y wget && \
    apt-get install -y gdebi && \
    apt-get install -y postgresql-8.4 && \
    apt-get install -y sshpass && \
    apt-get install -y git-core && \
    apt-get install -y apt-transport-https && \
    apt-get install -y net-tools && \
    apt-get clean
#apt-get install -y postgresql-8.4 && \
#apt-get install -y nano && \

# Set environment
RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8
#RUN env


# Configure users
RUN useradd --home /home/scidb --create-home --uid 1005 --group sudo --shell /bin/bash scidb && \
	echo 'root:bigdawg' | chpasswd && \
	echo 'postgres:bigdawg' | chpasswd && \
	echo 'scidb:bigdawg' | chpasswd && \
	echo 'bigdawg'  >> /home/scidb/pass.txt  && \
	mkdir /var/run/sshd && \
	mkdir /home/scidb/data && \
	mkdir /home/scidb/catalog

# Configure SSH and Postgres
RUN echo 'StrictHostKeyChecking no' >> /etc/ssh/ssh_config && \
	echo 'host  all all 255.255.0.0/16   md5' >> /etc/postgresql/8.4/main/pg_hba.conf


# Add files
ADD containerSetup.sh /root/containerSetup.sh
ADD conf /root/conf
ADD iquery.conf /home/scidb/.config/scidb/iquery.conf
ADD installPackages.R /home/scidb/installPackages.R
ADD startScidb.sh /home/scidb/startScidb.sh
ADD stopScidb.sh /home/scidb/stopScidb.sh
ADD scidb_docker.ini /home/scidb/scidb_docker.ini


RUN chown -R root:root /root/*   && \
	chown -R scidb:scidb /home/scidb/* && \
	chmod +x /root/*.sh /home/scidb/*.sh


# Restarting services
#RUN stop ssh
#RUN start ssh
#RUN /etc/init.d/postgresql restart


EXPOSE 22
EXPOSE 8083

# Added by kyle:
ENV USER scidb
ADD initScidb.sh /
COPY resources /src/main/resources
RUN /usr/sbin/sshd && \
	/root/containerSetup.sh
ADD istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar /

CMD /initScidb.sh

#CMD    ["/usr/sbin/sshd", "-D"]
