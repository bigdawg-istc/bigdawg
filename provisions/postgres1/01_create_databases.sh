#!/bin/bash

echo "create database mimic2 with owner pguser"
gosu postgres psql <<- EOSQL
    CREATE DATABASE mimic2 OWNER pguser;
EOSQL
    echo "****** role altered ******"

echo "create user pguser"
gosu postgres psql <<- EOSQL
    CREATE DATABASE test OWNER pguser;
EOSQL
    echo "****** role pguser created ******"
