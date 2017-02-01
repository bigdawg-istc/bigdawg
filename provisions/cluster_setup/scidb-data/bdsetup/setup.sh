#!/bin/bash

chmod 777 /home/scidb/bdsetup

su scidb <<'EOF'
source /home/scidb/.bashrc
cd /home/scidb/bdsetup

echo "Creating array 'myarray'..."
/opt/scidb/14.12/bin/csv2scidb -p N < s00124_wave_3255538_00011.reformatted.csv > datafile.scidb
/opt/scidb/14.12/bin/iquery -aq "CREATE ARRAY myarray < dim1:double, dim2: double > [i=0:*,1000000,0];"
/opt/scidb/14.12/bin/iquery -naq "load(myarray, '/home/scidb/bdsetup/datafile.scidb');"

echo "Creating array 'test_array_flat'"
printf "100|1|1\n101|1|2\n103|2|1\n104|2|2" > /home/scidb/test_array_flat.psv
/opt/scidb/14.12/bin/iquery -aq "
create array test_array_flat <val:int64, i:int64, j:int64> [i_=0:*,100000,0];
create array test_array <val:int64>[i=0:*,100000,0,j=0:*,100000,0];
load(test_array_flat, '/home/scidb/test_array_flat.psv', -2, 'csv:p',1);
store(redimension(test_array_flat, test_array), test_array);"

echo "Done..."
EOF