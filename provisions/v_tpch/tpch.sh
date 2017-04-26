#!/bin/sh
set -ex

RESULTS='results'
DBNAME='docker'
USER='dbadmin'


mkdir -p $RESULTS

# wait for the vertica instance to get ready
while !(/opt/vertica/bin/vsql -U dbadmin -l 2>/dev/null); do sleep 3 && echo "Trying to connect..."; done

/opt/vertica/bin/vsql -U dbadmin -c 'ALTER DATABASE docker SET StandardConformingStrings = 0;'

echo "preparing TPC-H database"

echo "  loading data"

/opt/vertica/bin/vsql -U $USER -h localhost $DBNAME -f dss/tpch-load.sql > $RESULTS/load.log 2> $RESULTS/load.err

echo "  creating primary keys"

/opt/vertica/bin/vsql -U $USER -h localhost $DBNAME -f dss/tpch-pkeys.sql > $RESULTS/pkeys.log 2> $RESULTS/pkeys.err

echo "  creating foreign keys"

/opt/vertica/bin/vsql -U $USER -h localhost $DBNAME -f dss/tpch-alter.sql > $RESULTS/alter.log 2> $RESULTS/alter.err

# echo "running TPC-H benchmark"

# /opt/vertica/bin/vsql -U $USER -h localhost $DBNAME -f working.sql > results.log 2> results.err

# echo "finished TPC-H benchmark"


