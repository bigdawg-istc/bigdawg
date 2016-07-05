FROM bigdawg/postgres-base

# Make sure to copy these files into the same folder as the Dockerfile before building.
# These files live in ../../src/main/resources/, but Docker doesn't ADD files
# from outside of the current build context.
ADD catalog /tmp/catalog
ADD monitor /tmp/monitor
ADD schemas /tmp/schemas
ADD create_logs_table.sql /tmp/

ADD 01_load_mimic_data.sh /docker-entrypoint-initdb.d/
