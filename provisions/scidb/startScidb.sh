#!/bin/bash
echo "********************************************"
echo "START SCIDB"
echo "********************************************"
export LC_ALL="en_US.UTF-8"
#yes | scidb.py initall scidb_docker
scidb.py startall scidb_docker
scidb.py status scidb_docker
