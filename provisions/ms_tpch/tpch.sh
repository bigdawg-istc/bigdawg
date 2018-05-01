#!/bin/bash
set -ex

RESULTS='results'
DBNAME='tpch'
USER='mysqluser'
PASSWORD='test'

mkdir -p $RESULTS

echo "preparing TPC-H database"

# mysql --user=$USER -v -p$PASSWORD -e "create database $DBNAME if not exists;"

echo "  loading data"

mysql --user=$USER -v -p$PASSWORD --local-infile=1 $DBNAME < ./tpch-load.sql 

echo "  creating foreign keys"

mysql --user=$USER -v -v -p$PASSWORD $DBNAME < ./tpch-alter.sql 

echo "  creating indexes"

mysql --user=$USER -v -p$PASSWORD $DBNAME < ./tpch-index.sql

#echo "running TPC-H benchmark"

# mysql --user=$USER -v -p$PASSWORD $DBNAME < working.sql > results.log 2> results.err
  
#echo "finished TPC-H benchmark"


