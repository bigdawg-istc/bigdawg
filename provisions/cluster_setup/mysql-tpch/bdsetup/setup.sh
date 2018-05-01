#!/bin/bash

cd /bdsetup

cd ~ && \
while !(mysqladmin ping); do sleep 3 && echo "Trying to connect..."; done && \
echo "Setting up tpch data..."
mysql --user=mysqluser -v -ptest -e "create database tpch;"

cd /bdsetup/ms_tpch

# Insert records and run benchmark
./tpch.sh