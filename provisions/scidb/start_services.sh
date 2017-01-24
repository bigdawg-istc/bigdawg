#!/bin/bash

/usr/sbin/sshd
/etc/init.d/postgresql start
su scidb <<'EOF'
cd ~
source .bashrc
yes | scidb.py initall scidb_docker
/home/scidb/./startScidb.sh

echo "Services are running..."
tail -f /dev/null