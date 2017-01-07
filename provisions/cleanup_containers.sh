echo
echo "===== Stopping and removing container: postgres-catalog ====="
docker rm -f bigdawg-postgres-catalog
echo
echo "===== Stopping and removing container: postgres-data1 ====="
docker rm -f bigdawg-postgres-data1
echo
echo "===== Stopping and removing container: postgres-data2 ====="
docker rm -f bigdawg-postgres-data2
echo
echo "===== Stopping and removing conainer: scidb-data ====="
docker rm -f bigdawg-scidb bigdawg/scidb
echo
echo "===== Stopping and removing containers for accumulo-data ====="
docker rm -f accumulo-data-zookeeper
docker rm -f accumulo-data-tserver0
docker rm -f accumulo-data-master
docker rm -f accumulo-data-proxy
echo
echo "===== Done. Remaining containers (both running and stopped state): =====
docker ps -a
