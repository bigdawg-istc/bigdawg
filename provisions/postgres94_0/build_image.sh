#!/bin/bash

# Todo: copy necessary files from src/main/resources
# cp -a src/main/resources/catalog provisions/postgres1/

cp -r src/main/resources provisions/postgres94_0/

echo "Copying the middleware jar to image build context"
cp -a target/istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar provisions/postgres94_0

echo "Building postgres94_0"
docker build -t bigdawg/postgres0 provisions/postgres94_0