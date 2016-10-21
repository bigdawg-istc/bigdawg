#!/usr/bin/env bash

echo "Creating Docker containers..."
echo "(n/m) Creating \"postgres1\" - Postgres server. Exposed at postgres1:5432"
docker create --name bigdawg-postgres1 -e POSTGRES_PASSWORD=test --net=bigdawg -p 5431:5432 bigdawg/postgres1
echo "(n/m) Creating \"postgres2\" - Postgres server. Exposed at postgres2:5432"
docker create --name bigdawg-postgres2 -e POSTGRES_PASSWORD=test --net=bigdawg -p 5430:5432 bigdawg/postgres2
# echo "(n/m) Creating \"scidb\" - scidb server. Exposed at scidb:1412"
# docker create --name scidb --net=bigdawg -p 1412:1412 scidb
# echo "(n/m) Creating \"accumulo\" - accumulo server. Exposed at :1412"
#  # hdfs ports are 50010 to 50090
#  # mapred part is 9000
#  # yarn port is 19888
#  # other ports are 8088, 49707, 2122
#  # Zookeeper Ports are 2181
#  # Accumulo ports are 4560, 9997, 9999, 12234, 50091, 50095
# docker create --name accumulo \
#     --net=bigdawg \
#     -p 50010:50010 \
#     -p 50020:50020 \
#     -p 50070:50070 \
#     -p 50075:50075 \
#     -p 50090:50090 \
#     -p 8020:8020 \
#     -p 9000:9000 \
#     -p 19888:19888 \
#     -p 8030:8030 \
#     -p 8031:8031 \
#     -p 8032:8032 \
#     -p 8033:8033 \
#     -p 8040:8040 \
#     -p 8042:8042 \
#     -p 8088:8088 \
#     -p 49707:49707 \
#     -p 2122:2122 \
#     -p 2181:2181 \
#     -p 4560:4560 \
#     -p 9997:9997 \
#     -p 9999:9999 \
#     -p 12234:12234 \
#     -p 50091:50091 \
#     -p 50095:50095 \
#     accumulo
# echo "(n/m) Creating \"maven\" - Java Maven container. Exposed at 0.0.0.0:8188"
# docker create --name maven \
#     --net=bigdawg \
#     -v /vagrant:/bigdawg \
#     maven
