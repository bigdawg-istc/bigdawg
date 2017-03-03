#!/bin/bash

# Build the catalog database
cd /bdsetup
psql -c "create database bigdawg_catalog"
psql -f catalog_ddl.sql -d bigdawg_catalog
psql -f catalog_inserts_mysql.sql -d bigdawg_catalog
psql -f monitor_ddl.sql -d bigdawg_catalog
psql -c "create database bigdawg_schemas"
psql -f mimic2_schemas_ddl.sql -d bigdawg_schemas
psql -c "create database logs owner pguser"
psql -f logs_ddl.sql -d logs
