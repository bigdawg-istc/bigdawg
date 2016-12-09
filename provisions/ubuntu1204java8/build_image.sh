#!/usr/bin/env bash
# Run this script from the bigdawgmiddle project root

echo "Copying the jar to docker build context"
cp target/istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar provisions/ubuntu1204java8

echo "Building docker images"
docker build -t bigdawg/ubuntu1204java8 provisions/ubuntu1204java8
