FROM ubuntu:14.04

# Install java
RUN apt-get update && \
	apt-get install -y software-properties-common && \
    add-apt-repository ppa:webupd8team/java -y && \
    apt-get update && \
    echo oracle-java7-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections && \
    apt-get install -y oracle-java8-installer

# Install optional utilities
RUN apt-get install -y curl && \
    apt-get install -y netcat && \
    apt-get install -y iputils-ping && \
    apt-get install -y net-tools

# Install necessary utilities
RUN apt-get update && \
	apt-get install wget apt-transport-https -y

# Install scidb
RUN wget -O- https://downloads.paradigm4.com/key | sudo apt-key add - && \
	echo deb "https://downloads.paradigm4.com/ ubuntu14.04/14.12/" >> /etc/apt/sources.list.d/scidb.list && \
	echo "deb-src https://downloads.paradigm4.com/ ubuntu14.04/14.12/" >> /etc/apt/sources.list.d/scidb.list && \
	apt-get update && \
	apt-get install scidb-14.12-installer -y

# Install and setup ssh
RUN apt-get install -y ssh && \
	sed -i 's/PermitRootLogin without-password/PermitRootLogin yes/g' /etc/ssh/sshd_config && \
	ssh-keygen -t rsa -N "" -C "your_email@example.com" -f /root/.ssh/id_rsa && \
	cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys && \
	sed -i 's/::1     localhost ip6-localhost ip6-loopback/#::1     localhost ip6-localhost ip6-loopback/g' /etc/ssh/sshd_config && \
	service ssh restart

# Setup postgres (included in scidb installer)
RUN echo "listen_addresses = '*'" >> /etc/postgresql/9.3/main/postgresql.conf && \
	echo "hostssl all all ::1/128 trust" >> /etc/postgresql/9.3/main/pg_hba.conf && \
	echo "host    all             postgres        127.0.0.1/32            md5" >> /etc/postgresql/9.3/main/pg_hba.conf && \
	echo "host    all             postgres        ::1/128                 md5" >> /etc/postgresql/9.3/main/pg_hba.conf && \
	service postgresql restart


# Copy the middleware jar (build script should move this here)
COPY PostgresParserTerms.csv /src/main/resources/PostgresParserTerms.csv
COPY SciDBParserTerms.csv /src/main/resources/SciDBParserTerms.csv
COPY istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar /

COPY edit_etchosts.py /
COPY start_services.sh /
CMD /start_services.sh