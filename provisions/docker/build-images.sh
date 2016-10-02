#!/bin/bash

# This script builds Docker containers for postgres, accuulo, and scidb
#   postgres-base: the root docker image
#   postgres1: builds on postgres-base. It should hold the mimic2 dataset
#

# if (( EUID != 0 )); then
#    echo "Script must be run as root."
#    exit 126
# fi

# # Default use case is to run BigDawg in a Vagrant VM. Nonstandard setups should run this script from the BigDawg working directory.
# cd /vagrant &> /dev/null

# Download mimic2 data to the base image, if necessary
if [ ! -f provisions/postgres-base/mimic2.pgd ]; then
    echo "Downloading mimic2.pgd"
    wget -O provisions/postgres-base/mimic2.pgd \
        https://bitbucket.org/adam-dziedzic/bigdawgdata/raw/6ade22253695bfeb33074e82929e83b52cb121f1/mimic2.pgd
else
    echo "mimic2.pgd exists. Skipping download"
fi

# copy necessary files for postgres1
cp -a src/main/resources/catalog provisions/postgres1/
cp -a src/main/resources/monitor provisions/postgres1/
cp -a src/main/resources/schemas provisions/postgres1/
cp -a src/main/resources/create_logs_table.sql provisions/postgres1/

# Build the project's Docker images if you don't want to pull the prebuilt ones from Docker Hub
echo "Building Docker images..."

echo "Building postgres-base (1/6)"
docker build -t bigdawg/postgres-base provisions/postgres-base/

echo ""
echo "Building postgres1 (2/6)"
docker build -t bigdawg/postgres1 provisions/postgres1/

echo ""
echo "Building postgres2 (3/6)"
docker build -t bigdawg/postgres2 provisions/postgres2/

# echo ""
# echo "Building scidb (4/6)"
# docker build -t scidb provisions/scidb/

# echo ""
# echo "Building accumulo (5/6)"
# docker build -t accumulo provisions/accumulo-1.6.5

# echo ""
# echo "Building maven (6/6)"
# docker build -t maven /vagrant/
