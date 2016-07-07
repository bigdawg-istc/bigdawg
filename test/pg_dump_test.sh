1;2802;0c#!/bin/bash
data_folder=/home/adam/data/
file_from_postgres=${data_folder}from_postgres_waveform_.bin
file_from_postgres_csv=${data_folder}from_postgres_waveform_.csv
file_to_scidb=${data_folder}to_scidb_waveform.bin
file_from_scidb=${data_folder}from_scidb_waveform.bin
csv_file=${data_folder}waveform_1GB.csv
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

time_script() {
    START_TIME=$(date +%s.%N)
    time bash -x $1 $2 $3 $4 $5 $6
    END_TIME=$(date +%s.%N)

    DIFF_TIME=$(echo "$END_TIME - $START_TIME" | bc)

    LOG_MSG="script $1, $DIFF_TIME"
    TIMESTAMP=$(date -d"$CURRENT +$MINUTES minutes" '+%F_%T.%N_%Z')
    echo $TIMESTAMP,$LOG_MSG,size,$size >> ${1}.log
}

prepare_environment() {
    rm -f ${postgres_pipe}
    rm -f ${scidb_pipe}

    mkfifo ${postgres_pipe}
    mkfifo ${scidb_pipe}

    chmod a+rw ${postgres_pipe}
    chmod a+rw ${scidb_pipe}

    # uncomment restart_databases for real tests
    #bash restart_databases.sh ${postgresql_version} ${pg_bin} ${postgresql_data_location}
    saved_dir=$(pwd)
    cd ${bigdawg}/installation
    bash stop_postgres.sh
    #bash stop_scidb.sh

    sync
    echo drop caches; echo 3 | sudo tee /proc/sys/vm/drop_caches;

    bash start_postgres.sh
    #bash start_scidb.sh
    cd ${saved_dir}
    #bash clean_table_array.sh ${user} ${database} ${postgresql_table} ${pg_path} ${port} ${scidb_array}
}

start_sar() {
    TIMESTAMP=$(date -d"$CURRENT +$MINUTES minutes" '+%F_%T.%N_%Z')
    sar -b -d -q -n DEV -r -u -W -o /home/adam/data-loading/sar/2016-01-26/${1}_${BENCHMARK}_scale_factor_${size}_data_files_in_${DATA_STORE}_database_in_${STORE}_parallel_level_${parallel_level}_${TIMESTAMP}.sar 1 > /dev/null 2>&1 &
    SAR_PID=$!
    sleep 3
}

finish_sar() {
    kill -9 $SAR_PID # sar blocks on wait
}

function create_table {
    psql -U ${user} -d ${database} -p ${port} -a -c "drop table if exists ${postgresql_table}; create table ${postgresql_table} (a bigint not null, b bigint not null, val double precision not null)";
}

function load_csv_postgres {
    prepare_environment
    # load csv data to postgresql
    START=$(date +%s.%N)
    psql -U ${user} -d ${database} -p ${port} -a -c "drop table if exists ${postgresql_table}"
    #psql -U adam -d test -p 5431 -a -c "begin; create table test_waveform (a bigint not null, b bigint not null, val double precision not null); copy test_waveform from '/home/adam/data/waveform_001GB.csv' with (format csv, header true, freeze); commit;"
    psql -U ${user} -d ${database} -p ${port} -a -c "begin; create table ${postgresql_table} (a bigint not null, b bigint not null, val double precision not null); copy ${postgresql_table} from '${csv_file}' with (format csv, header true, freeze); commit;"
    END=$(date +%s.%N)
    DIFF_POSTGRES_CSV=$(echo "$END - $START" | bc)

    select="select count(*) from ${postgresql_table}"
    echo $select
    tuples=$(echo $select | psql -d ${database} -p ${port} -P t -P format=unaligned)
    TIMESTAMP=$(date -d"$CURRENT +$MINUTES minutes" '+%F_%T.%N_%Z')
    echo $TIMESTAMP, Loading time to postgresql,${DIFF_POSTGRES_CSV}, number of tuples in ${postgresql_table}, ${tuples}, size, $size >> potgres_load.log

    prepare_environment
    START=$(date +%s.%N)
    time psql -U ${user} -d ${database} -p ${port} -a -c "copy ${postgresql_table} to '${csv_file}_copy' with (format csv, header true, freeze)"
    END=$(date +%s.%N)
    DIFF_POSTGRES_CSV_DUMP=$(echo "$END - $START" | bc)
    size_file_copy=`du -s -h ${csv_file}_copy | awk '{print $1}'`
    echo size of the csv exported from postgresql file: ${size_file_copy},DIFF_POSTGRES_CSV_DUMP,size,$size >> postgres_csv_dump.log
    rm ${csv_file}_copy

    prepare_environment
    START=$(date +%s.%N)
    time psql -U ${user} -d ${database} -p ${port} -a -c "copy ${postgresql_table} to '${csv_file}_copy_bin' with (format binary, freeze)"
    END=$(date +%s.%N)
    DIFF_POSTGRES_BIN_DUMP=$(echo "$END - $START" | bc)
    size_file_copy=`du -s -h ${csv_file}_copy_bin | awk '{print $1}'`
    echo size of the binary exported from postgresql file: ${size_file_copy},DIFF_POSTGRES_BIN_DUMP, size, $size >> postgres_bin_dump.log

    prepare_environment
    # load csv data to postgresql
    START=$(date +%s.%N)
    psql -U ${user} -d ${database} -p ${port} -a -c "drop table if exists ${postgresql_table}"
    #psql -U adam -d test -p 5431 -a -c "begin; create table test_waveform (a bigint not null, b bigint not null, val double precision not null); copy test_waveform from '/home/adam/data/waveform_001GB.csv' with (format csv, header true, freeze); commit;"
    psql -U ${user} -d ${database} -p ${port} -a -c "begin; create table ${postgresql_table} (a bigint not null, b bigint not null, val double precision not null); copy ${postgresql_table} from '${csv_file}_copy_bin' with (format binary, freeze); commit;"
    END=$(date +%s.%N)
    DIFF_POSTGRES_bin=$(echo "$END - $START" | bc)

    select="select count(*) from ${postgresql_table}"
    echo $select
    tuples=$(echo $select | psql -d ${database} -p ${port} -P t -P format=unaligned)
    TIMESTAMP=$(date -d"$CURRENT +$MINUTES minutes" '+%F_%T.%N_%Z')
    echo $TIMESTAMP, Loading time to postgresql bin,${DIFF_POSTGRES_bin}, number of tuples ${postgresql_table}, ${tuples}, size, $size >> potgres_bin_load.log

    rm ${csv_file}_copy_bin
}

for size in 10; do # 5 10 15 20 30 1 2 5 10 15 20 30; 1 2 5 10 15 20 1 2 5 10 15 20 1 2 5 10 15 20 30 ;  2 5 10 15 20 30 1 2 5 10 15 20 30 1 2 5 10 15 20 30 001
    csv_file=${data_folder}waveform_${size}GB.csv
    #scp adam@francisco:${csv_file} ${csv_file}
    #load_csv_postgres

    size_file=`du -s --block-size=1M ${csv_file} | awk '{print $1}'`
    echo size of the csv file: $size_file
    #rm ${csv_file}

    for compress in 0 1 2 3 4 5 6 7 8 9; do
	export LD_LIBRARY_PATH=${pg_path}/lib:$LD_LIBRARY_PATH
	export PATH=${pg_path}/lib:$PATH
	export PATH=${pg_path}/bin:$PATH

	# between postgres
	prepare_environment

	time_script pg_dump_restore.sh ${compress} ${size} ${size_file}
	time_script pg_dump_restore_pipe.sh ${compress} ${size} ${size_file}

    done;
done;
