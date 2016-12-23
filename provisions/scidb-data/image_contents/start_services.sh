#!/bin/bash

/usr/sbin/sshd
/etc/init.d/postgresql start
su scidb <<'EOF'
cd ~
source .bashrc
yes | scidb.py initall scidb_docker
/home/scidb/./startScidb.sh
echo "Services are running..."

echo "Creating array 'myarray'..."
csv2scidb -p N < /scidb_data/s00124_wave_3255538_00011.reformatted.csv > /home/scidb/datafile.scidb
iquery -aq "CREATE ARRAY myarray < dim1:double, dim2: double > [i=0:*,1000000,0];"
echo "Inserting data"
iquery -naq "load(myarray, '/home/scidb/datafile.scidb');"
echo "Done..."
tail -f /dev/null
