#!/bin/bash

pguser=pguser

# Create user
#   -s: Superuser
#   -d: User will be able to create databases
#   -e: Echo the commands
gosu postgres createuser -s -d -e ${pguser}
gosu postgres psql -c "alter role ${pguser} with password 'test'"

# catalog database
gosu postgres psql -c "create database bigdawg_catalog"
gosu postgres psql -f /tmp/catalog/catalog_ddl.sql -d bigdawg_catalog
gosu postgres psql -f /tmp/catalog/catalog_inserts.sql -d bigdawg_catalog
gosu postgres psql -f /tmp/monitor/monitor_ddl.sql -d bigdawg_catalog

## schemas database
gosu postgres psql -c "create database bigdawg_schemas"
gosu postgres psql -f /tmp/schemas/mimic2_schemas_ddl.sql -d bigdawg_schemas
#gosu postgres psql -f /tmp/schemas/tpch_schemas_ddl.sql -d bigdawg_schemas

# mimic2 database
gosu postgres psql -c "create database mimic2 owner ${pguser}"
gosu postgres psql -f /tmp/mimic2/mimic2.pgd -U ${pguser} -d mimic2  # tables are created inside the mimic2.pgd script

# tpch database
gosu postgres psql -c "create database tpch owner ${pguser}"
# todo: create tables and load data here

# logs database
gosu postgres psql -c "create database logs owner ${pguser}"
gosu postgres psql -f /tmp/logs/logs_ddl.sql -d logs

# test database
gosu postgres psql -c "create database test owner ${pguser}"

echo "*****\n==> Done with database setup.\n*****"
