#!/bin/bash
docker stop scidb_dev1
docker rm scidb_dev1
docker rmi scidb_dev_img
docker build --rm=true --tag="scidb_dev_img" .

docker run --privileged -d --name="scidb_dev1" -p 49901:22  --expose=22 --expose=1239 --expose=5432 scidb_dev_img

#ssh -p 49901 root@localhost
