#!/usr/bin/env bash
# Run this script from the bigdawgmiddle project root

echo "Building docker image"
docker build -t bigdawg/ubuntu1404java8 provisions/ubuntu1404java8
