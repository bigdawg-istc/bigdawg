#!/bin/bash

pguser=pguser
log_db=logs
catalog_db=bigdawg_catalog

# Create user
#   -s: Superuser
#   -d: User will be able to create databases
#   -e: Echo the commands
gosu postgres createuser -s -d -e ${pguser}
gosu postgres psql -c "alter role ${pguser} with password 'test'"

# Create catalog database
gosu postgres psql -c "create database ${catalog_db}"
gosu postgres psql -f /tmp/catalog/bigdawg_ddl.sql -d ${catalog_db}
gosu postgres psql -f /tmp/catalog/inserts.docker.sql -d ${catalog_db}

# Create tables for monitor in the catalog database
gosu postgres psql -f /tmp/monitor/monitor.sql -d ${catalog_db}

# schemas database
gosu postgres psql -c "create database bigdawg_schemas" -d template1
gosu postgres psql -f /tmp/schemas/bigdawg_schemas_setup.sql -d bigdawg_schemas
gosu postgres psql -f /tmp/mimic2.pgd -U ${pguser} -d bigdawg_schemas

# Create log database
gosu postgres psql -c "create database ${log_db} owner ${pguser}"
gosu postgres psql -f /tmp/create_logs_table.sql -d ${log_db}

# Create test database
gosu postgres psql -c "create database test owner ${pguser}"

echo "All done."
