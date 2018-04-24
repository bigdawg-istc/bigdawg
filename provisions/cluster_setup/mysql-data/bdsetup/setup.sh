#!/bin/bash
cd ~ && \
while !(mysqladmin ping); do sleep 3 && echo "Trying to connect..."; done && \
echo "Setting up mimic2 data..."
mysql --user=mysqluser -v -ptest -e "create database mimic2v26; use mimic2v26; source /home/mysql/mimic2_mysql.sql;" && \
mysql --user=mysqluser -v -ptest -e "create database test;"