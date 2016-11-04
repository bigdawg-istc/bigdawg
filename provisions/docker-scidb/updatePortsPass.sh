#!/bin/bash
##############################################################
#REPLACE DEFAULT PORTS AND PASSWORDS
##############################################################

# Docker image name
find ./* -exec sed -i 's/scidb_img/scidb_modis_img/g' {} \;
# Container name
find ./* -exec sed -i 's/scidb1/scidb_modis1/g' {} \;
# Container's passwords for root, postgres & scidb users
find ./* -exec sed -i 's/xxxx.xxxx.xxxx/yyyy.yyyy.yyyy/g' {} \;
# Container's SSH port
find ./* -exec sed -i 's/49901/49971/g' {} \;
# Container's passwords for root, postgres & scidb users
# Container's POSTGRESQL port
find ./* -exec sed -i 's/49902/49972/g' {} \;
# Container's SHIM secured port - see conf
find ./* -exec sed -i 's/49903/49973/g' {} \;
# Container's SCIDB port
find ./* -exec sed -i 's/49904/49974/g' {} \;
