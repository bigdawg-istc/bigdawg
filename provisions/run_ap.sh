#!/bin/bash

echo
echo "===== Running postgres-catalog ====="
docker run -d --net=bigdawg -h bigdawg-postgres-catalog -p 5400:5400 -p 8080:8080 --name bigdawg-postgres-catalog bigdawg/postgres-catalog
echo
echo "===== Running postgres-data1 ====="
docker run -d --net=bigdawg -h bigdawg-postgres-data1 -p 5401:5401 --name bigdawg-postgres-data1 bigdawg/postgres-data1
echo
echo "===== Running accumulo-data ====="
accumulo-data/start_accumulo_cluster.sh
