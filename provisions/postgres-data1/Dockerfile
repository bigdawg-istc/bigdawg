FROM bigdawg/postgres

COPY image_contents /image_contents/

# subsequent mimic script wants this directory to exist
USER root
RUN mkdir -p /var/lib/postgresql/9.4/main/mimic2v26_dat && \
	chown postgres /var/lib/postgresql/9.4/main/mimic2v26_dat

# Insert the first 1k records of the mimic dataset, create "test" database
USER postgres
RUN	cd ~ && \
	tar -xvf /image_contents/mimic2_flatfiles.tar.gz --directory ~ && \
	cd MIMIC-Importer-2.6 && \
	/etc/init.d/postgresql start && \
	./import.sh -s 0 -e 999 && \
	psql -c "ALTER DATABASE \"MIMIC2\" RENAME TO mimic2" && \
	psql -c "create database test owner pguser" && \
	/etc/init.d/postgresql stop

# Set some environment variables that define the runtime behavior in the "start_services.sh" script.
# These can be overridden by the docker run command by using -e flag
ENV PGPORT 5401
ENV BDHOST bigdawg-postgres-data1
EXPOSE 5401

COPY istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar /image_contents/

CMD /image_contents/start_services.sh