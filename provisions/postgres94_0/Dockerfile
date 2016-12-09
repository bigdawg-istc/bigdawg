FROM bigdawg/postgres94

ADD catalog /tmp/catalog
ADD monitor /tmp/monitor
ADD schemas /tmp/schemas
ADD logs/ /tmp/logs
COPY resources /src/main/resources

USER postgres

RUN /etc/init.d/postgresql start &&\
	echo "host all  all    0.0.0.0/0  md5" >> /etc/postgresql/9.4/main/pg_hba.conf && \
	echo "listen_addresses='*'" >> /etc/postgresql/9.4/main/postgresql.conf && \
	psql -c "CREATE USER pguser WITH SUPERUSER PASSWORD 'test';" && \
	/etc/init.d/postgresql stop

RUN /etc/init.d/postgresql start && \
	psql -c "create database bigdawg_catalog" && \
	psql -f /tmp/catalog/catalog_ddl.sql -d bigdawg_catalog && \
	psql -f /tmp/catalog/catalog_inserts.sql -d bigdawg_catalog && \
	psql -f /tmp/monitor/monitor_ddl.sql -d bigdawg_catalog && \
	psql -c "create database bigdawg_schemas" && \
	psql -f /tmp/schemas/mimic2_schemas_ddl.sql -d bigdawg_schemas && \
	psql -f /tmp/schemas/tpch_schemas_ddl.sql -d bigdawg_schemas && \
	psql -c "create database logs owner pguser" && \
	psql -f /tmp/logs/logs_ddl.sql -d logs && \
	/etc/init.d/postgresql stop

# Add volumes to allow backup of config, logs and databases
VOLUME  ["/etc/postgresql", "/var/log/postgresql", "/var/lib/postgresql"]

# Postgres listens on this port. See https://www.postgresql.org/docs/9.4/static/app-postgres.html
ENV PGPORT 5400
EXPOSE 5400

ADD setup_postgres0.sh /
ADD istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar /

CMD /setup_postgres0.sh


