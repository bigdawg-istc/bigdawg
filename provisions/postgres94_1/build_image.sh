#!/usr/bin/env bash

# Download mimic2 data if necessary
if [ ! -f provisions/postgres94_1/mimic2.pgd.tar.gz ]; then
    echo "Downloading mimic2.pgd"
    wget https://bitbucket.org/adam-dziedzic/bigdawgdata/raw/6ade22253695bfeb33074e82929e83b52cb121f1/mimic2.pgd
    echo "Adding mimic2.pgd to mimic2.pgd.tar.gz"
    tar -cvzf mimic2.pgd.tar.gz mimic2.pgd && mv mimic2.pgd.tar.gz provisions/postgres94_1/ && rm mimic2.pgd
else
    echo "mimic2.pgd.tar.gz exists. Skipping download"
fi

# Todo: copy necessary files
# cp -a src/main/resources/catalog provisions/postgres1/
cp -r -a src/main/resources provisions/postgres94_1/

echo "Copying the middleware jar to image build context"
cp -a target/istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar provisions/postgres94_1

echo "Building postgres94_1"
docker build -t bigdawg/postgres1 provisions/postgres94_1
