#!/bin/bash

# This script assumes it's being run from the same directory that it resides in

# Download the mimic data if necessary
mkdir -p image_contents 
if [ -f "image_contents/mimic2_flatfiles.tar.gz" ]
then
	echo "Mimic data already exists. Skipping download"
else
	echo "Downloading the mimic2 dataset"
	curl -o image_contents/mimic2_flatfiles.tar.gz --create-dirs https://physionet.org/mimic2/demo/mimic2_flatfiles.tar.gz
fi

echo "Copying the middleware jar"
cp ../../target/istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar .

echo "Building image"
docker build --rm -t bigdawg/postgres-data2 .