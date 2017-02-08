#!/bin/bash

# Modify etc/hosts so scidb starts.
cp /etc/hosts ~/hosts.new
sed -i 's/::1/#::1/g' ~/hosts.new
cp -f ~/hosts.new /etc/hosts

# start postgres, ssh, scidb
service postgresql restart
service ssh restart
service postgresql restart

# start scidb
cd /opt/scidb/14.12/bin
echo y | ./scidb.py initall single_server
./scidb.py startall single_server

# Keep the container active and running
echo "Keeping container active..."
tail -f /dev/null