#!/bin/bash 
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
port=5431 # 5431 5432
host=localhost
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
    bash stop_scidb.sh

    sync
    echo drop caches; echo 3 | sudo tee /proc/sys/vm/drop_caches;

    bash start_postgres.sh
    bash start_scidb.sh
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
    csv_file=${data_folder}waveform_${size}GB.csv
    scp adam@francisco:${csv_file} ${csv_file}

    prepare_environment
    # load csv data to postgresql
    START=$(date +%s.%N)
    psql -U ${user} -d ${database} -p ${port} -a -c "drop table if exists ${postgresql_table}"
    psql -U ${user} -d ${database} -p ${port} -a -c "begin; create table ${postgresql_table} (a bigint not null, b bigint not null, val double precision not null); copy ${postgresql_table} from '${csv_file}' with (format csv, freeze); commit;"
    END=$(date +%s.%N)
    DIFF_POSTGRES_CSV=$(echo "$END - $START" | bc)

    select="select count(*) from ${postgresql_table}"
    echo $select
    tuples=$(echo $select | psql -d ${database} -p ${port} -P t -P format=unaligned)
    TIMESTAMP=$(date -d"$CURRENT +$MINUTES minutes" '+%F_%T.%N_%Z')
    echo $TIMESTAMP, Loading time to postgresql,${DIFF_POSTGRES_CSV}, number of tuples in ${postgresql_table}, ${tuples}, size, $size >> potgres_load.log

    rm ${csv_file}
}

for size in 0001 001; do # 5 10 15 20 30 1 2 5 10 15 20 30; 1 2 5 10 15 20 1 2 5 10 15 20 1 2 5 10 15 20 30 ;  2 5 10 15 20 30 1 2 5 10 15 20 30 1 2 5 10 15 20 30 001
    export LD_LIBRARY_PATH=${pg_path}/lib:$LD_LIBRARY_PATH
    export PATH=${pg_path}/lib:$PATH
    export PATH=${pg_path}/bin:$PATH

    load_csv_postgres

    cd ${bigdawg}

    # between postgres
    prepare_environment
    mvn -Dtest=WaveformTest#testFromPostgresToPostgres test -P dev
    psql -U ${user} -d ${database} -p 5430 -a -c "drop table if exists ${postgresql_table};"

    # raw postgres
    prepare_environment
    START=$(date +%s.%N)
    psql -U ${user} -d ${database} -p ${port} -a -c "copy ${postgresql_table} to '${postgres_pipe}' with (format binary, freeze)" &
    psql -U ${user} -d ${database} -p 5430 -a -c  "begin; create table ${postgresql_table} (a bigint not null, b bigint not null, val double precision not null); copy ${postgresql_table} from '${postgres_pipe}' with (format binary, freeze); commit;" &
    wait
    END=$(date +%s.%N)
    DIFF_BIN_MIGRATION_PSQL=$(echo "$END - $START" | bc)
    echo The data migration time was ${DIFF_BIN_MIGRATION}

    select="select count(*) from ${postgresql_table}"
    echo $select
    tuples=$(echo $select | psql -d ${database} -p 5430 -P t -P format=unaligned)
    TIMESTAMP=$(date -d"$CURRENT +$MINUTES minutes" '+%F_%T.%N_%Z')
    echo $TIMESTAMP, migration time,${DIFF_BIN_MIGRATION_PSQL}, tuples, ${tuples}, size, $size >> postgres_raw_migration_binary.log  
    psql -U ${user} -d ${database} -p 5430 -a -c "drop table if exists ${postgresql_table};"

    # from table to flat array in scidb

    iquery -aq "remove(${scidb_array});";
    iquery -aq "create array ${scidb_array} <a:int64,b:int64,val:double>[k=0:*,1000000,0];";
    prepare_environment
    mvn -Dtest=WaveformTest#testFromPostgresToSciDBCsv test -P dev

    iquery -aq "remove(${scidb_array});";
    iquery -aq "create array ${scidb_array} <a:int64,b:int64,val:double>[k=0:*,1000000,0];";
    prepare_environment
    mvn -Dtest=WaveformTest#testFromPostgresToSciDBBin test -P dev

    # from flat array
    create_table
    prepare_environment
    mvn -Dtest=WaveformTest#testFromSciDBToPostgresCsv test -P dev

    create_table
    prepare_environment
    mvn -Dtest=WaveformTest#testFromSciDBToPostgresBin test -P dev

    # from table to multidimensional array
    load_csv_postgres

    iquery -aq "remove(${scidb_array});";
    iquery -aq "create array ${scidb_array} <val:double> [a=0:*,1000,0,b=0:*,1000,0];"
    prepare_environment
    mvn -Dtest=WaveformTest#testFromPostgresToSciDBCsv test -P dev

    iquery -aq "remove(${scidb_array});";
    iquery -aq "create array ${scidb_array} <val:double> [a=0:*,1000,0,b=0:*,1000,0];"
    prepare_environment
    mvn -Dtest=WaveformTest#testFromPostgresToSciDBBin test -P dev

    # from multidimensional array

    create_table
    prepare_environment
    mvn -Dtest=WaveformTest#testFromSciDBToPostgresCsv test -P dev

    create_table
    prepare_environment
    mvn -Dtest=WaveformTest#testFromSciDBToPostgresBin test -P dev

    # my pure binary data migration from PostgreSQL to SciDB

    prepare_environment

    # #non-direct binary data migration
    load_csv_postgres

    iquery -aq "remove(${scidb_array});";
    iquery -aq "create array ${scidb_array} <a:int64,b:int64,val:double>[k=0:*,1000000,0];";
    START=$(date +%s.%N)
    psql -U ${user} -d ${database} -p ${port} -a -c "copy ${postgresql_table} to '${postgres_pipe}' with (format binary, freeze)" &

    #cat ${postgres_pipe} > /tmp/postgres_file &

    #cat ${postgres_file} > ${postgres_pipe} &

    #cat ${postgres_pipe} > /tmp/postgres_file2

    postgres2scidb -i ${postgres_pipe} -o ${scidb_pipe} -f${format_migrator} &

    #cat ${scidb_pipe} > /tmp/scidb_file &

    #cat ${scidb_file} > ${scidb_pipe}
    iquery -naq "load(${scidb_array},'${scidb_pipe}',-2,'(${format_scidb})')" &

    wait

    END=$(date +%s.%N)
    DIFF_BIN_MIGRATION=$(echo "$END - $START" | bc)
    echo The data migration time was ${DIFF_BIN_MIGRATION}

    scidb_items1=$(iquery -q "select count(*) from ${scidb_array}" | sed -n 2p | cut -d " " -f2)

    TIMESTAMP=$(date -d"$CURRENT +$MINUTES minutes" '+%F_%T.%N_%Z')
    echo $TIMESTAMP, migration time,${DIFF_BIN_MIGRATION}, scidb items, ${scidb_items1}, size, $size >> migration_binary.log    

done;
