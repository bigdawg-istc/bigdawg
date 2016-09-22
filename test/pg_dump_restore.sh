#!/bin/bash
data_folder=/home/adam/data/
file_from_postgres=${data_folder}from_postgres_waveform_.bin
file_from_postgres_csv=${data_folder}from_postgres_waveform_.csv
file_to_scidb=${data_folder}to_scidb_waveform.bin
file_from_scidb=${data_folder}from_scidb_waveform.bin
csv_file=${data_folder}waveform_001GB.csv
postgres_pipe=/tmp/from_postgres
postgres_file=/tmp/postgres_file
scidb_pipe=/tmp/to_scidb
scidb_file=/tmp/scidb_file
postgresql_table="test_waveform" #test_waveform3 #test_int
scidb_array="test_waveform" #test_waveform3 #test_int
format_migrator="int64,int64,double" # int64
format_scidb="int64,int64,double" # int64
user=adam
database=test
postgresql_database=$database
postgresql_user=$user
postgresql_version=pgsql-9.4.5  #pgsql-9.3.9 # pgsql-9.3.9 pgsql4scidb
#pg_path=/home/adam/databases/PostgreSQL/${postgresql_version}
pg_path=/home/adam/bigdawgmiddle/installation/Downloads/postgres1
pg_bin=${pg_path}/bin/
psql=${pg_bin}psql
port=5432 # 5431 5432
host=postgres1
#postgresql_data_location=/home/adam/data-database/PostgreSQL/${postgresql_version}/data
postgresql_data_location=/home/adam/databases/PostgreSQL/dataPostgreSQL/${postgresql_version}/data
BENCHMARK=mimic2
DATA_STORE=ssd
STORE=ssd
parallel_level=1
temp_file=/tmp/test_waveform.bin
format_bin_scidb="int64,int64,double"
bigdawg=/home/adam/bigdawgmiddle

pg_path1=/home/adam/bigdawgmiddle/installation/Downloads/postgres1
pg_bin1=${pg_path}/bin/
psql1=${pg_bin}psql
port1=5431 # 5431 5432

pg_path2=/home/adam/bigdawgmiddle/installation/Downloads/postgres2
pg_bin2=${pg_path}/bin/
psql2=${pg_bin}psql
port2=5432 # 5431 5432

host=postgres2

compress=${1:-0}
size=${2:-"unknown"}
size_file=${3:-"unkonwn"}

psql -U ${user} -d ${database} -p 5430 -a -c "drop table if exists ${postgresql_table}; create table ${postgresql_table} (a bigint not null, b bigint not null, val double precision not null);"
#time pg_dump --data-only --file /home/adam/data/pg_dump.out --format custom --schema public --no-owner --table test_waveform -v --compress 0 --lock-wait-timeout=10 --no-security-labels --no-tablespaces --section=data --dbname=test -p 5431 -h localhost -U postgres --no-password
dump_file=/home/adam/data/pg_dump.out
rm -f ${dump_file}
#mkfifo ${dump_file}
chmod a+rw ${dump_file}

START=$(date +%s.%N)
time pg_dump --file ${dump_file} --format custom --schema public --no-owner --table ${postgresql_table} -v --compress ${compress} --lock-wait-timeout=10 --no-security-labels --no-tablespaces --section=data --dbname=${database} -p 5431 -h localhost -U adam --no-password --clean --if-exists --encoding=SQL_ASCII
END=$(date +%s.%N)

DIFF=$(echo "$END - $START" | bc)

MSG="dump, $DIFF"
echo $MSG

size_dump=`du -s --block-size=1M ${dump_file} | awk '{print $1}'`
echo size of the backup: $size_dump

TIMESTAMP=$(date -d"$CURRENT +$MINUTES minutes" '+%F_%T.%N_%Z')

echo $TIMESTAMP,data size,$size,size dump,$size_dump,$START,$END,$MSG >> dump.log

START=$(date +%s.%N)
time pg_restore -v --host localhost --port 5430 -U adam --no-password --single-transaction -d test "${dump_file}"
END=$(date +%s.%N)

DIFF=$(echo "$END - $START" | bc)

MSG="restore, $DIFF"
echo $MSG
LOG_MSG="${LOG_MSG},${MSG}"
#psql -U ${user} -d ${database} -p 5430 -a -c "select count(*) from test_waveform"
select="select count(*) from test_waveform"
echo $select
tuples=$(echo $select | psql -d ${database} -p 5430 -U ${user} -P t -P format=unaligned)

echo $TIMESTAMP,data size,$size,size dump,$size_dump,size file,${size_file},tuples,${tuples},$START,$END,$MSG >> restore.log


