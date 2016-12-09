#!/bin/bash

# Todo: copy necessary files from src/main/resources
# cp -a src/main/resources/catalog provisions/postgres1/

echo "Copying the middleware jar to image build context"
cp -a target/istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar provisions/scidb
cp -r src/main/resources provisions/scidb/


echo "Building scidb"
docker build -t bigdawg/scidb provisions/scidb