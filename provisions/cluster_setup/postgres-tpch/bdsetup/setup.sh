#!/bin/bash
set -ex
cd /bdsetup

# # Build dbgen & qgen
# cd dbgen
# sudo apt-get update
# sudo apt-get install -y build-essential
# sudo apt-get -y install make
# cp makefile.suite Makefile
# sed -i 's/CC[ ]*=/CC = gcc/' Makefile
# sed -i 's/DATABASE[ ]*=/DATABASE = ORACLE/' Makefile
# sed -i 's/MACHINE[ ]*=/MACHINE = LINUX/' Makefile
# sed -i 's/WORKLOAD[ ]*=/WORKLOAD = TPCH/' Makefile
# make clean && make

# Setup the postgres database
cd pg_tpch

# Create "tpch" database. Run as postgres.
su postgres <<'EOF'
whoami
psql -c "create database tpch owner pguser"
EOF

# Insert records
./setup.sh