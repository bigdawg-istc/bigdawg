#!/bin/bash

chmod 777 /home/scidb/bdsetup

su scidb <<'EOF'
source /home/scidb/.bashrc
cd /home/scidb/bdsetup

echo "Creating array 'myarray'..."
/opt/scidb/14.12/bin/csv2scidb -p N < s00124_wave_3255538_00011.reformatted.csv > datafile.scidb
/opt/scidb/14.12/bin/iquery -aq "CREATE ARRAY myarray < dim1:double, dim2: double > [i=0:*,1000000,0];"
/opt/scidb/14.12/bin/iquery -naq "load(myarray, '/home/scidb/bdsetup/datafile.scidb');"
echo "Done..."
EOF