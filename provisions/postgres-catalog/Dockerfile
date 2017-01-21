FROM bigdawg/postgres

COPY image_contents /image_contents/

# Build the catalog database
RUN /etc/init.d/postgresql start && \
	psql -c "create database bigdawg_catalog" && \
	psql -f /image_contents/catalog_ddl.sql -d bigdawg_catalog && \
	psql -f /image_contents/catalog_inserts.sql -d bigdawg_catalog && \
	psql -f /image_contents/monitor_ddl.sql -d bigdawg_catalog && \
	psql -c "create database bigdawg_schemas" && \
	psql -f /image_contents/mimic2_schemas_ddl.sql -d bigdawg_schemas && \
	psql -c "create database logs owner pguser" && \
	psql -f /image_contents/logs_ddl.sql -d logs && \
	/etc/init.d/postgresql stop

# Set some environment variables that define the runtime behavior in the "start_services.sh" script.
# These can be overridden by the docker run command by using -e flag
ENV PGPORT 5400
EXPOSE 5400
ENV BDHOST bigdawg-postgres-catalog

COPY istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar /image_contents/

CMD /image_contents/start_services.sh