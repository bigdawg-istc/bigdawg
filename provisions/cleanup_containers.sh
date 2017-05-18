#!/bin/bash
echo
echo "====================================================================="
echo "===== bigdawg-postgres-catalog: Stopping and removing container ====="
echo "====================================================================="
docker rm -f bigdawg-postgres-catalog
echo
echo "==================================================================="
echo "===== bigdawg-postgres-data1: Stopping and removing container ====="
echo "==================================================================="
docker rm -f bigdawg-postgres-data1
echo
echo "==================================================================="
echo "===== bigdawg-postgres-data2: Stopping and removing container ====="
echo "==================================================================="
docker rm -f bigdawg-postgres-data2
echo
echo "========================================================="
echo "===== bigdawg-scidb: Stopping and removing conainer ====="
echo "========================================================="
docker rm -f bigdawg-scidb-data
echo
echo "============================================================="
echo "===== accumulo-data-*: Stopping and removing containers ====="
echo "============================================================="
docker rm -f bigdawg-accumulo-zookeeper
docker rm -f bigdawg-accumulo-tserver0
docker rm -f bigdawg-accumulo-master
docker rm -f bigdawg-accumulo-proxy
docker rm -f bigdawg-accumulo-namenode
echo
echo "========================================================="
echo "===== bigdawg-sstore: Stopping and removing conainer ====="
echo "========================================================="
docker rm -f bigdawg-sstore-data
echo
echo "=========================================================================="
echo "===== Done. Final container status (both running and stopped state): ====="
echo "=========================================================================="
docker ps -a
