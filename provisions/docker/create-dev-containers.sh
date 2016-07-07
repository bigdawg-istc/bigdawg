docker network create bigdawg_dev 2> /dev/null

echo "Creating \"bigdawg\" Docker network if needed..."
docker network create bigdawg 2> /dev/null

echo "Creating Docker containers..."
echo "(n/m) Creating \"postgres1\" - Postgres server. Exposed at postgres1:5432"
docker create --name postgres1 \
    --net=bigdawg \
    -p 5431:5432 \
    postgres1
echo "(n/m) Creating \"postgres2\" - Postgres server. Exposed at postgres2:5432"
docker create --name postgres2 \
    --net=bigdawg \
    -p 5430:5432 \
    postgres2
echo "(n/m) Creating \"maven\" - Java Maven container. Exposed at 0.0.0.0:8188"
docker create --name maven \
    --net=bigdawg \
    -v /vagrant:/bigdawg \
    maven
