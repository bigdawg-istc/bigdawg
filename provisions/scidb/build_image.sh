#!/bin/bash

echo "Building image..."
docker build --rm -t bigdawg/scidb .
