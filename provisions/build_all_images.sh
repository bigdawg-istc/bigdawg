#!/bin/bash

echo "Creating the bigdawg docker network if not present"
$(docker network inspect bigdawg &>/dev/null) || {   docker network create bigdawg; }

echo
echo "============================="
echo "===== Building postgres ====="
echo "============================="
cd postgres
./build_image.sh
cd ..

echo
echo "====================================="
echo "===== Building postgres-catalog ====="
echo "====================================="
cd postgres-catalog
./build_image.sh
cd ..

echo
echo "==================================="
echo "===== Building postgres-data1 ====="
echo "==================================="
cd postgres-data1
./build_image.sh
cd ..

echo
echo "==================================="
echo "===== Building postgres-data2 ====="
echo "==================================="
cd postgres-data2
./build_image.sh
cd ..

echo
echo "=========================="
echo "===== Building scidb ====="
echo "=========================="
cd scidb
./build_image.sh
cd ..

echo
echo "==============================="
echo "===== Building scidb-data ====="
echo "==============================="
cd scidb-data
./build_image.sh
cd ..

echo
echo "=================================="
echo "===== Building accumulo-data ====="
echo "=================================="
cd accumulo-data
./build_image.sh
cd ..

echo
echo "================="
echo "===== Done. ====="
echo "================="