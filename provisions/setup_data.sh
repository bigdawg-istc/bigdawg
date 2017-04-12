
# echo
# echo "========================================"
# echo "===== Pulling images from Dockerhub====="
# echo "========================================"
# echo "==> postgres"
# docker pull bigdawg/postgres
# echo "==> scidb"
# docker pull bigdawg/scidb
# echo "==> postgres"
# docker build --rm -t bigdawg/postgres postgres/

# echo "==> mysql"
# docker build --rm -t bigdawg/mysql mysql/

echo "==> vertica"
docker build --rm -t bigdawg/vertica vertica/

# echo "==> postgres-postgres-tpch"
# docker rm -f bigdawg-postgres-tpch
# docker run -d --net=bigdawg -h bigdawg-postgres-tpch -p 5401:5401 -e "PGPORT=5401" -e "BDHOST=bigdawg-postgres-tpch" --name bigdawg-postgres-tpch bigdawg/postgres

# # postgres-data1
# docker cp cluster_setup/postgres-tpch/bdsetup bigdawg-postgres-tpch:/
# docker cp pg_tpch bigdawg-postgres-tpch:/bdsetup/
# docker exec --user=root bigdawg-postgres-tpch /bdsetup/setup.sh

# echo "==> bigdawg-mysql-tpch"
# docker rm -f bigdawg-mysql-tpch
# docker run -d --net=bigdawg -h bigdawg-mysql-tpch -p 5401:5401 -e "PGPORT=5401" -e "BDHOST=bigdawg-mysql-tpch" --name bigdawg-mysql-tpch bigdawg/mysql

# # postgres-data1
# docker cp cluster_setup/mysql-tpch/bdsetup bigdawg-mysql-tpch:/
# docker cp ms_tpch bigdawg-mysql-tpch:/bdsetup/
# docker exec --user=root bigdawg-mysql-tpch /bdsetup/setup.sh

echo "==> bigdawg-vertica-tpch"
docker rm -f bigdawg-vertica-tpch
docker run -d --net=bigdawg -h bigdawg-vertica-tpch -p 5433:5433 --name bigdawg-vertica-tpch bigdawg/vertica

# postgres-data1
docker cp cluster_setup/vertica-tpch/bdsetup bigdawg-vertica-tpch:/
docker cp v_tpch bigdawg-vertica-tpch:/bdsetup/
docker exec --user=root bigdawg-vertica-tpch /bdsetup/setup.sh

echo
echo "================="
echo "===== Done. ====="
echo "================="
