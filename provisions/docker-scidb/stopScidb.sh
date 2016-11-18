#!/bin/bash
echo "*************************************"
echo "STOP SCIDB"
echo "*************************************"
export LC_ALL=en_US.UTF-8
scidb.py stopall scidb_docker
