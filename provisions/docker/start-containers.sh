#!/bin/bash

if (( EUID != 0 )); then
   echo "Script must be run as root."
   exit 126
fi

echo "Starting postgres1..."
docker start postgres1
echo "Starting postgres2..."
docker start postgres2
echo "Waiting 5 seconds for db to spin up..."
sleep 5
echo "Starting maven..."
docker start maven


echo "Done."
