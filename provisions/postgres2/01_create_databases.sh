#!/bin/bash

echo "Creating database mimic2_copy with owner pguser"
gosu postgres psql <<- EOSQL
    CREATE DATABASE mimic2_copy owner pguser template=template1;
EOSQL
    echo "****** mimic2_copy db created ******"

echo "Creating database test with owner pguser"
gosu postgres psql <<- EOSQL
    CREATE DATABASE test owner pguser template=template1;
EOSQL
    echo "****** test db created ******"