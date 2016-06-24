#!/bin/bash

echo "alter role postgres with password 'test'"
gosu postgres psql <<- EOSQL
    ALTER ROLE postgres WITH PASSWORD 'test'
EOSQL
    echo "****** role altered ******"

echo "create user pguser"
gosu postgres psql <<- EOSQL
    CREATE ROLE pguser
EOSQL
    echo "****** role pguser created ******"
