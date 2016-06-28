from postgres:9.4.5

# Make sure wget is available for downloading mimic2.pgd
RUN apt-get update && \
    apt-get install -y wget

# Make sure to copy these files into the same folder as the Dockerfile before building.
# These files live in ../../src/main/resources/, but Docker doesn't ADD files
# from outside of the current build context.
ADD catalog /tmp/catalog
ADD monitor /tmp/monitor
ADD create_logs_table.sql /tmp/

ADD 00_require_password_for_remote_connections.sh /docker-entrypoint-initdb.d/
ADD 01_load_mimic_data.sh /docker-entrypoint-initdb.d/
