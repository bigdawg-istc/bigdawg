SETUP_DATA=$1
echo "==> copying jar"
rm postgres/istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar
cp ../target/istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar postgres

echo
echo "=============================================================="
echo "===== Creating the bigdawg docker network if not present ====="
echo "=============================================================="
$(docker network inspect bigdawg &>/dev/null) || {   docker network create bigdawg; }

# echo
# echo "========================================"
# echo "===== Pulling images from Dockerhub====="
# echo "========================================"
# echo "==> postgres"
# docker pull bigdawg/postgres
# echo "==> scidb"
# docker pull bigdawg/scidb
echo "==> postgres"
docker build --rm -t bigdawg/postgres postgres/

echo "==> postgres-catalog"
docker rm -f bigdawg-postgres-catalog
docker run -d --net=bigdawg -h bigdawg-postgres-catalog -p 5400:5400 -p 8080:8080 -e "PGPORT=5400" -e "BDHOST=bigdawg-postgres-catalog" --name bigdawg-postgres-catalog bigdawg/postgres
if [ $SETUP_DATA = "d" ]
then
    echo "==> postgres-data1"
    docker rm -f bigdawg-postgres-tpch
    docker run -d --net=bigdawg -h bigdawg-postgres-tpch -p 5401:5401 -e "PGPORT=5401" -e "BDHOST=bigdawg-postgres-tpch" --name bigdawg-postgres-tpch bigdawg/postgres
    echo "==> bigdawg-vertica-tpch"
    docker rm -f bigdawg-vertica-tpch
    docker run -d --net=bigdawg -h bigdawg-vertica-tpch -p 5430:5433 --name bigdawg-vertica-tpch bigdawg/vertica


    # postgres-data1
    docker cp cluster_setup/postgres-tpch/bdsetup bigdawg-postgres-tpch:/
    docker cp pg_tpch bigdawg-postgres-tpch:/bdsetup/
    docker cp tpch-data bigdawg-postgres-tpch:/bdsetup/pg_tpch
    docker exec --user=root bigdawg-postgres-tpch /bdsetup/setup.sh

    # vertica-tpch
    docker cp cluster_setup/vertica-tpch/bdsetup bigdawg-vertica-tpch:/
    docker cp v_tpch bigdawg-vertica-tpch:/bdsetup/
    docker cp tpch-data bigdawg-vertica-tpch:/bdsetup/v_tpch
    docker exec --user=root bigdawg-vertica-tpch /bdsetup/setup.sh
fi

# postgres-catalog
docker exec -u root bigdawg-postgres-catalog mkdir -p /src/main/resources
docker cp ../src/main/resources/PostgresParserTerms.csv bigdawg-postgres-catalog:/src/main/resources
docker cp ../src/main/resources/SciDBParserTerms.csv bigdawg-postgres-catalog:/src/main/resources
docker cp cluster_setup/postgres-catalog/bdsetup bigdawg-postgres-catalog:/
docker exec bigdawg-postgres-catalog /bdsetup/setup_tpch.sh catalog_inserts_tpch.sql


echo
echo "======================================="
echo "===== Starting BigDAWG Middleware ====="
echo "======================================="
# docker exec -d bigdawg-scidb-data java -classpath "istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar" istc.bigdawg.Main bigdawg-scidb-data
docker exec bigdawg-postgres-catalog java -classpath "istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar" istc.bigdawg.Main bigdawg-postgres-catalog

echo
echo "================="
echo "===== Done. ====="
echo "================="
