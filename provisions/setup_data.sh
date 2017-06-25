#!/bin/bash
# -----------------------------------
# Script for setting up the TPCH data
# -----------------------------------
set -ex
echo
echo "=============================================================="
echo "===== Creating the bigdawg docker network if not present ====="
echo "=============================================================="
$(docker network inspect bigdawg &>/dev/null) || { xdocker network create bigdawg; }
# echo
# echo "========================================"
# echo "===== Pulling images from Dockerhub====="
# echo "========================================"
# echo "==> postgres"
# docker pull bigdawg/postgres
# echo "==> scidb"
# docker pull bigdawg/scidb
# echo "==> postgres"
docker build --rm -t bigdawg/postgres postgres/

# echo "==> mysql"
# docker build --rm -t bigdawg/mysql mysql/

# echo "==> vertica"
# docker build --rm -t bigdawg/vertica vertica/
if [ $1 = "p" ]
then
    echo "==> postgres-data-tpch"
    #docker rm -f bigdawg-postgres-tpch
    docker run -d --net=bigdawg -h bigdawg-postgres-tpch --volumes-from dbstore -p 5403:5401 -e "PGPORT=5401" -e "BDHOST=bigdawg-postgres-tpch" --name bigdawg-postgres-tpch bigdawg/postgres

    # postgres-data1
    docker cp cluster_setup/postgres-tpch/bdsetup bigdawg-postgres-tpch:/
    docker cp pg_tpch bigdawg-postgres-tpch:/bdsetup/
    #docker cp tpch-data bigdawg-postgres-tpch:/bdsetup/pg_tpch
    #docker exec --user=root bigdawg-postgres-tpch /bdsetup/setup.sh
fi

if [ $1 = "m" ]
then
    echo "==> bigdawg-mysql-tpch"
    docker rm -f bigdawg-mysql-tpch
    docker run -d --net=bigdawg -h bigdawg-mysql-tpch -p 5401:5401 -e "PGPORT=5401" -e "BDHOST=bigdawg-mysql-tpch" --name bigdawg-mysql-tpch bigdawg/mysql

    docker cp cluster_setup/mysql-tpch/bdsetup bigdawg-mysql-tpch:/
    docker cp ms_tpch bigdawg-mysql-tpch:/bdsetup/
    docker cp tpch-data bigdawg-mysql-tpch:/bdsetup/ms_tpch/
    docker exec --user=root bigdawg-mysql-tpch /bdsetup/setup.sh
fi

if [ $1 = "v" ]
then
    echo "==> bigdawg-vertica-tpch"
    #docker rm -f bigdawg-vertica-tpch
    docker run -d --net=bigdawg --volumes-from dbstore -h bigdawg-vertica-tpch -p 5430:5433 --name bigdawg-vertica-tpch bigdawg/vertica

    docker cp cluster_setup/vertica-tpch/bdsetup bigdawg-vertica-tpch:/
    docker cp v_tpch bigdawg-vertica-tpch:/bdsetup/
    #docker cp tpch-data bigdawg-vertica-tpch:/bdsetup/v_tpch
    #docker exec --user=root bigdawg-vertica-tpch /bdsetup/setup.sh
fi

echo
echo "================="
echo "===== Done. ====="
echo "================="

