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
docker rm -f bigdawg-scidb bigdawg/scidb
echo
echo "============================================================="
echo "===== accumulo-data-*: Stopping and removing containers ====="
echo "============================================================="
docker rm -f accumulo-data-zookeeper
docker rm -f accumulo-data-tserver0
docker rm -f accumulo-data-tserver1
docker rm -f accumulo-data-master
docker rm -f accumulo-data-proxy
docker rm -f accumulo-data-namenode
echo
echo "=========================================================================="
echo "===== Done. Final container status (both running and stopped state): ====="
echo "=========================================================================="
docker ps -a
