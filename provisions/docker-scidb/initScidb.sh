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
EOF

echo "Finished initialization"
tail -f /dev/null