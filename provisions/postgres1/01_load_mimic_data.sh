#!/bin/bash

echo "Downloading mimic2.pgd"
gosu postgres wget -O /tmp/mimic2.pgd \
    https://bitbucket.org/adam-dziedzic/bigdawgdata/raw/6ade22253695bfeb33074e82929e83b52cb121f1/mimic2.pgd

pguser=pguser
log_db=logs
catalog_db=bigdawg_catalog

gosu postgres psql -c "alter role postgres with password 'test'"
gosu postgres createuser -s -e -d ${pguser}

gosu postgres psql -c "create database ${catalog_db}"
gosu postgres psql -f /tmp/catalog/bigdawg_ddl.sql -d ${catalog_db}
gosu postgres psql -f /tmp/catalog/inserts.sql -d ${catalog_db}

gosu postgres psql -c "alter role ${pguser} with password 'test'"

gosu postgres psql -c "create database mimic2 owner ${pguser}"
gosu postgres psql -f /tmp/mimic2.pgd -U ${pguser} -d mimic2

# tables for monitor
gosu postgres psql -f /tmp/monitor/monitor.sql -d ${catalog_db}

# log_db
gosu postgres psql -c "create database ${log_db} owner ${pguser}"
gosu postgres psql -f /tmp/create_logs_table.sql -d ${log_db}

# tests
gosu postgres psql -c "create database test owner ${pguser}"
echo "All done."