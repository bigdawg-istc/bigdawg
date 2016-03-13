user=${1:-adam}
database=${2:-test}
postgresql_table=${3:-test_waveform}
pgpath=${4:-/home/adam/databases/PostgreSQL/pgsql-9.4.4}
port=${5:-5431}
scidb_array=${6:-test_waveform}

export LD_LIBRARY_PATH=${pg_path}/lib:$LD_LIBRARY_PATH 
export PATH=${pg_path}/lib:$PATH
export PATH=${pg_path}/bin:$PATH

psql -U ${user} -p ${port} -d ${database} -a -c "drop table if exists ${postgresql_table};"
psql -U ${user} -p ${port} -d template1 -a -c "drop database if exists test;";
psql -U ${user} -p ${port} -d template1 -a -c "create database test;";

iquery -aq "remove(${scidb_array});";
iquery -aq "create array ${scidb_array} <a:int64,b:int64,val:double>[k=0:*,1000000,0];";

iquery -aq "remove(target);";
iquery -aq "create array target <val:double> [a=0:*,1000,0,b=0:*,1000,0];";

