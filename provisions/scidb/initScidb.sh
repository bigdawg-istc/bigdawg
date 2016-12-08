#!/bin/bash

# >/dev/null 2>&1

THISUSER=`whoami`
echo "running as user: ${THISUSER}"

echo "starting ssh"
/usr/sbin/sshd

echo "starting postgres"
/etc/init.d/postgresql start

echo "starting scidb"
su scidb <<'EOF'
cd ~
source ~/.bashrc
#********************************************************
echo "***** ***** Starting SciDB..."
#********************************************************
yes | scidb.py initall scidb_docker
scidb.py startall scidb_docker

# Insert test data. Requires test_array in bigdawg_schemas
iquery -aq "create array test_array <val:double> [i=1:10,5,0,j=1:10,5,0];"
iquery -aq "store(build(test_array,i*j),test_array)"

EOF
echo "scidb initialized"

cd /
java -classpath "/istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar" istc.bigdawg.Main bigdawg-scidb

echo "Finished initialization"
tail -f /dev/null