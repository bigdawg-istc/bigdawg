echo "==> copying jar"
rm postgres/istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar
cp ../target/istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar postgres

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
echo "==> postgres-data1"
docker rm -f bigdawg-postgres-tpch
docker run -d --net=bigdawg -h bigdawg-postgres-tpch -p 5401:5401 -e "PGPORT=5401" -e "BDHOST=bigdawg-postgres-tpch" --name bigdawg-postgres-tpch bigdawg/postgres

# postgres-catalog
docker exec -u root bigdawg-postgres-catalog mkdir -p /src/main/resources
docker cp ../src/main/resources/PostgresParserTerms.csv bigdawg-postgres-catalog:/src/main/resources
docker cp ../src/main/resources/SciDBParserTerms.csv bigdawg-postgres-catalog:/src/main/resources
docker cp cluster_setup/postgres-catalog/bdsetup bigdawg-postgres-catalog:/
docker exec bigdawg-postgres-catalog /bdsetup/setup_tpch.sh

# postgres-data1
docker cp cluster_setup/postgres-tpch/bdsetup bigdawg-postgres-tpch:/
docker cp dbgen bigdawg-postgres-tpch:/bdsetup/
docker cp pg_tpch bigdawg-postgres-tpch:/bdsetup/
docker exec --user=root bigdawg-postgres-tpch /bdsetup/setup.sh

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
