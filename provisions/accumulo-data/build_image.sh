#!/bin/bash

echo "Copying the middleware jar"
cp ../../target/istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar .

echo "Building image"
docker build --rm -t bigdawg/accumulo-data .