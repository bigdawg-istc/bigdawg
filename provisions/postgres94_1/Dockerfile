FROM bigdawg/postgres94

COPY mimic2.pgd.tar.gz /
COPY resources /src/main/resources
RUN tar -xvf mimic2.pgd.tar.gz

USER postgres

RUN /etc/init.d/postgresql start &&\
	echo "host all  all    0.0.0.0/0  md5" >> /etc/postgresql/9.4/main/pg_hba.conf && \
	echo "listen_addresses='*'" >> /etc/postgresql/9.4/main/postgresql.conf && \
	psql -c "CREATE USER pguser WITH SUPERUSER PASSWORD 'test';" && \
	/etc/init.d/postgresql stop

RUN /etc/init.d/postgresql start && \
	psql -c "create database mimic2 owner pguser" && \
	psql -f mimic2.pgd -d mimic2 && \
	psql -c "create database tpch owner pguser" && \
	psql -c "create database test owner pguser" && \
	/etc/init.d/postgresql stop && \
	echo "*****\n==> Done with database setup.\n*****"
	# todo: create tables and load data for tpch
	# psql -f mimic2.pgd -U pguser -d mimic2 && \

# Add VOLUMEs to allow backup of config, logs and databases
VOLUME  ["/etc/postgresql", "/var/log/postgresql", "/var/lib/postgresql"]

# Postgres listens on this port. See https://www.postgresql.org/docs/9.4/static/app-postgres.html
ENV PGPORT 5401
EXPOSE 5401

ADD setup_postgres1.sh /
ADD istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar /

CMD /setup_postgres1.sh



