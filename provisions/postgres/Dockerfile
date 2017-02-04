FROM ubuntu:14.04

# Install java and other utils
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y software-properties-common && \
    add-apt-repository ppa:webupd8team/java -y && \
    apt-get update && \
    echo oracle-java7-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections && \
    apt-get install -y oracle-java8-installer && \
    apt-get install -y curl && \
    apt-get install -y netcat && \
    apt-get install -y iputils-ping && \
    apt-get install -y net-tools && \
    apt-get clean

# Install postgres 9.4
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ trusty-pgdg main" > /etc/apt/sources.list.d/pgdg.list  && \
	wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add - && \
	sudo apt-get update && \
	apt-get install -y postgresql-9.4 && \
	psql --version && \
	apt-get update && \
	apt-get install -y software-properties-common postgresql-9.4 postgresql-client-9.4 postgresql-contrib-9.4 && \
	chmod -R 0700 /etc/ssl/private && \
	chown -R postgres /etc/ssl/private


RUN rm /etc/postgresql/9.4/main/pg_hba.conf && \
	echo "host all all all trust" >> /etc/postgresql/9.4/main/pg_hba.conf && \
	echo "local all all trust" >> /etc/postgresql/9.4/main/pg_hba.conf && \
	chmod 777 /etc/postgresql/9.4/main/pg_hba.conf && \
	ls -l /etc/postgresql/9.4/main/pg_hba.conf

# Configure postgres
USER postgres
RUN /etc/init.d/postgresql start &&\
	psql -c "ALTER USER postgres PASSWORD 'test';" && \
	echo "listen_addresses='*'" >> /etc/postgresql/9.4/main/postgresql.conf && \
	psql -c "CREATE USER pguser WITH SUPERUSER CREATEDB LOGIN REPLICATION PASSWORD 'test';" && \
	/etc/init.d/postgresql stop

COPY start_services.sh /

# Copy over build artifacts
COPY PostgresParserTerms.csv /src/main/resources/PostgresParserTerms.csv
COPY SciDBParserTerms.csv /src/main/resources/SciDBParserTerms.csv
COPY istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar /

CMD /start_services.sh