#!/bin/bash 
data_folder=/home/adam/data/
file_from_postgres=${data_folder}from_postgres_waveform_.bin
file_from_postgres_csv=${data_folder}from_postgres_waveform_.csv
file_to_scidb=${data_folder}to_scidb_waveform.bin
file_from_scidb=${data_folder}from_scidb_waveform.bin
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
postgresql_version=pgsql5scidb  #pgsql-9.3.9 # pgsql-9.3.9 pgsql4scidb
pg_bin=/home/adam/databases/PostgreSQL/${postgresql_version}/bin/
psql=${pg_bin}psql
port=5431 # 5431 5432
host=localhost
postgresql_data_location=/home/adam/data-database/PostgreSQL/${postgresql_version}/data

time_script() {
    START_TIME=$(date +%s.%N)
    time bash $1 $2 $3 $4 $5 $6
    END_TIME=$(date +%s.%N)

    DIFF_TIME=$(echo "$END_TIME - $START_TIME" | bc)

    LOG_MSG="script $1, $DIFF_TIME"
    TIMESTAMP=$(date -d"$CURRENT +$MINUTES minutes" '+%F_%T.%N_%Z')
    echo $TIMESTAMP,$LOG_MSG,scale,$scale >> online.log
}

# repeat experiments n times
#for n in 1 2 3 4 5; do
for size in 1 2 5 10 15 20 30 1 2 5 10 15 20 30 1 2 5 10 15 20 30; do # 5 10 15b 20 30 1 2 5 10 15 20 30; 1 2 5 10 15 20 1 2 5 10 15 20 1 2 5 10 15 20 30

    csv_file=${data_folder}waveform_${size}GB.csv

    scp adam@francisco:${csv_file} ${csv_file}

    rm -f ${postgres_pipe}
    rm -f ${scidb_pipe}

    mkfifo ${postgres_pipe}
    mkfifo ${scidb_pipe}

    chmod a+rw ${postgres_pipe}
    chmod a+rw ${scidb_pipe}

    bash restart_databases.sh ${postgresql_version} ${pg_bin} ${postgresql_data_location}

    iquery -aq "remove(${scidb_array});";
    iquery -aq "create array ${scidb_array} <a:int64,b:int64,val:double>[k=0:*,1000000,0];";

    psql -U ${user} -d ${database} -h ${host} -p ${port} -a -c "drop table if exists ${postgresql_table};"
    psql -U ${user} -d template1 -h ${host} -p ${port} -a -c "drop database if exists test;";
    psql -U ${user} -d template1 -h ${host} -p ${port} -a -c "create database test;";

    # bash restart_databases.sh ${postgresql_version} ${pg_bin} ${postgresql_data_location}

    bash restart_databases.sh ${postgresql_version} ${pg_bin} ${postgresql_data_location}

    # dump postgresql bin
    START=$(date +%s.%N)
    psql -U ${user} -d ${database} -h ${host} -p ${port} -a -c "copy ${postgresql_table} to '${file_from_postgres}' with (format binary, freeze)"
    END=$(date +%s.%N)
    DIFF_DUMP_BIN_POSTGRES=$(echo "$END - $START" | bc)    

    POSTGRESQL_BIN_EXPORT_FILE_SIZE=$(ls -lah ${file_from_postgres} | cut -d" " -f5)    
    echo POSTGRESQL_BIN_EXPORT_FILE_SIZE,${POSTGRESQL_BIN_EXPORT_FILE_SIZE}

    # remove data from postgres 
    psql -U ${user} -d ${database} -h ${host} -p ${port} -a -c "drop table ${postgresql_table};"
    psql -U ${user} -d template1 -h ${host} -p ${port} -a -c "drop database test;";
    psql -U ${user} -d template1 -h ${host} -p ${port} -a -c "create database test;";
    POSTGRES_SIZE_BEFOR_BIN_LOADING=$(du -h --max-depth=0 ${postgresql_data_location} | cut -d$'\t' -f1)
    echo POSTGRES_SIZE_BEFOR_BIN_LOADING,${POSTGRES_SIZE_BEFOR_BIN_LOADING}

    bash restart_databases.sh ${postgresql_version} ${pg_bin} ${postgresql_data_location}

    # load binary data to postgresql
    START=$(date +%s.%N)
    psql -U ${user} -d ${database} -p ${port} -a -c "begin; create table ${postgresql_table} (a bigint not null, b bigint not null, val double precision not null); copy ${postgresql_table} from '${file_from_postgres}' with (format binary, freeze); commit;"
    END=$(date +%s.%N)
    DIFF_POSTGRES_BIN=$(echo "$END - $START" | bc)
    echo Loading time to postgresql ${DIFF_POSTGRES_BIN}

    POSTGRES_SIZE_BIN_LOADING=$(du -h --max-depth=0 ${postgresql_data_location} | cut -d$'\t' -f1)
    echo POSTGRES_SIZE_BIN_LOADING,$POSTGRES_SIZE_BIN_LOADING

    bash restart_databases.sh ${postgresql_version} ${pg_bin} ${postgresql_data_location}

    # dump postgresql csv
    START=$(date +%s.%N)
    psql -U ${user} -d ${database} -h ${host} -p ${port} -a -c "copy ${postgresql_table} to '${file_from_postgres_csv}' with (format csv, freeze)"
    END=$(date +%s.%N)
    DIFF_DUMP_CSV_POSTGRES=$(echo "$END - $START" | bc)    

    # clean the disk
    POSTGRES_CSV_FILE_SIZE=$(ls -lah ${file_from_postgres_csv} | cut -d" " -f5)
    POSTGRES_CSV_LINE_NUMBER=$(wc -l ${file_from_postgres_csv} | cut -d" " -f1)
    rm -f ${file_from_postgres_csv}

    # remove data from postgres 
    psql -U ${user} -d ${database} -h ${host} -p ${port} -a -c "drop table ${postgresql_table};"
    psql -U ${user} -d template1 -h ${host} -p ${port} -a -c "drop database test;";
    psql -U ${user} -d template1 -h ${host} -p ${port} -a -c "create database test;";

    bash restart_databases.sh ${postgresql_version} ${pg_bin} ${postgresql_data_location}

    # pure binary loading to scidb
    iquery -aq "remove(${scidb_array});";
    iquery -aq "create array ${scidb_array} <a:int64,b:int64,val:double>[k=0:*,1000000,0];";

    START=$(date +%s.%N)
    iquery -naq "load(${scidb_array},'${file_from_postgres}',-2,'(${format_scidb})')"
    END=$(date +%s.%N)
    DIFF_BIN_SCIDB_LOAD=$(echo "$END - $START" | bc)    

    scidb_items_bin_loading=$(iquery -q "select count(*) from ${scidb_array}" | sed -n 2p | cut -d " " -f2)

    rm -f ${file_from_postgres}

    # direct binary data migration from scidb to postgres    
    START=$(date +%s.%N)
    iquery -naq "save(${scidb_array},'${postgres_pipe}',-2,'(${format_scidb})')" &
    psql -U ${user} -d ${database} -h ${host} -p ${port} -a -c "begin; create table ${postgresql_table} (a bigint not null, b bigint not null, val double precision not null); copy ${postgresql_table} from '${postgres_pipe}' with (format binary, freeze);commit;" &

    wait
    END=$(date +%s.%N)
    DIFF_BIN_DIRECT_MIGRATION_TO_POSTGRES=$(echo "$END - $START" | bc)
    echo The data migration time was ${DIFF_BIN_DIRECT_MIGRATION_TO_POSTGRES}

    POSTGRES_SIZE_AFTER_DIRECT_BIN_MIGRATION_FROM_SCIDB=$(du -h --max-depth=0 ${postgresql_data_location} | cut -d$'\t' -f1)
    echo $POSTGRES_SIZE_AFTER_DIRECT_BIN_MIGRATION_FROM_SCIDB

    # count # of rows
    # select="select count(*) from ${postgresql_table}"
    # echo $select
    # tuples_bin=$(echo $select | $psql -d ${database} -P t -P format=unaligned)
    # echo "number of tuples", $tuples_bin

    # remove data from postgres 
    psql -U ${user} -d ${database} -h ${host} -p ${port} -a -c "drop table ${postgresql_table};"
    psql -U ${user} -d template1 -h ${host} -p ${port} -a -c "drop database test;";
    psql -U ${user} -d template1 -h ${host} -p ${port} -a -c "create database test;";

    bash restart_databases.sh ${postgresql_version} ${pg_bin} ${postgresql_data_location}

    # save binary data from scidb
    START=$(date +%s.%N)
    iquery -naq "save(${scidb_array},'${file_from_scidb}',-2,'(${format_scidb})')"
    END=$(date +%s.%N)
    DIFF_BIN_EXPORT_SCIDB=$(echo "$END - $START" | bc)

    SCIDB_BIN_EXPORT_FILE_SIZE=$(ls -lah ${file_from_scidb} | cut -d" " -f5)    
    echo SCIDB_BIN_EXPORT_FILE_SIZE,${SCIDB_BIN_EXPORT_FILE_SIZE}

    bash restart_databases.sh ${postgresql_version} ${pg_bin} ${postgresql_data_location}

    iquery -aq "remove(${scidb_array});"
    iquery -aq "create array ${scidb_array} <a:int64,b:int64,val:double>[k=0:*,1000000,0];"

    rm -f ${file_from_scidb}

    # iquery -aq "remove(${scidb_array});";
    # iquery -aq "create array ${scidb_array} <a:int64,b:int64,val:double>[k=0:*,1000000,0];";

    # # binary export from PostgreSQL and binary transformation from PostgreSQL binary format to SciDB binary format (one pipe)
    # psql -U ${user} -d ${database} -a -c "copy ${postgresql_table} to '${postgres_pipe}' with (format binary)" &
    # postgres2scidb -i ${postgres_pipe} -o ${file_to_scidb} -f${format_migrator} &

    # wait

    # END=$(date +%s.%N)
    # DIFF_BIN_POSTGRES_EXPORT_TRANSFORMATION=$(echo "$END - $START" | bc)
    # echo DIFF_BIN_POSTGRES_EXPORT_TRANSFORMATION ${DIFF_BIN_POSTGRES_EXPORT_TRANSFORMATION}

    # SCIDB_BIN_FILE_SIZE=$(ls -lah ${file_to_scidb} | cut -d" " -f5)    
    # echo SCIDB_BIN_FILE_SIZE,${SCIDB_BIN_FILE_SIZE}

    # rm -f ${file_to_scidb}

    # # dump postgresql bin

    # START=$(date +%s.%N)
    # psql -U ${user} -d ${database} -h ${host} -p ${port} -a -c "copy ${postgresql_table} to '${file_from_postgres}' with (format binary, freeze)"
    # END=$(date +%s.%N)
    # DIFF_DUMP_BIN_POSTGRES=$(echo "$END - $START" | bc)    
    # echo DIFF_DUMP_BIN_POSTGRES,$DIFF_DUMP_BIN_POSTGRES

    # # pure binary data transformation
    # sync
    # echo drop caches; echo 3 | sudo tee /proc/sys/vm/drop_caches;

    # START=$(date +%s.%N)
    # postgres2scidb -i ${file_from_postgres} -o ${file_to_scidb} -f${format_migrator}
    # END=$(date +%s.%N)
    # DIFF_BIN_TRANSFORMATION=$(echo "$END - $START" | bc)    
    # echo DIFF_BIN_TRANSFORMATION, ${DIFF_BIN_TRANSFORMATION}

    # sync
    # echo drop caches; echo 3 | sudo tee /proc/sys/vm/drop_caches;

    # # clean
    # rm -f ${postgres_pipe}
    # rm -f ${scidb_pipe}

    # mkfifo ${postgres_pipe}
    # mkfifo ${scidb_pipe}

    # chmod a+rw ${postgres_pipe}
    # chmod a+rw ${scidb_pipe}

    # bash restart_databases.sh

    # iquery -aq "remove(${scidb_array});";
    # iquery -aq "create array ${scidb_array} <a:int64,b:int64,val:double>[k=0:*,1000000,0];";

    # # binary transformation from PostgreSQL to binary format to SciDB binary format and binary loading to SciDB (one pipe)
    # postgres2scidb -i ${file_from_postgres} -o ${scidb_pipe} -f${format_migrator} &
    # iquery -naq "load(${scidb_array},'${scidb_pipe}',-2,'(${format_scidb})')" &

    # wait

    # END=$(date +%s.%N)
    # DIFF_BIN_TRANSFORMATION_LOAD_BIN_SCIDB=$(echo "$END - $START" | bc)
    # echo DIFF_BIN_TRANSFORMATION_LOAD_BIN_SCIDB ${DIFF_BIN_POSTGRES_EXPORT_TRANSFORMATION}

    # scidb_items_transformation_scidb_bin_load=$(iquery -q "select count(*) from ${scidb_array}" | sed -n 2p | cut -d " " -f2)
    # echo scidb_items_transformation_scidb_bin_load,${scidb_items_transformation_scidb_bin_load}

    # rm -r ${file_from_postgres}

    # clean the pipes
    rm -f ${postgres_pipe}
    rm -f ${scidb_pipe}

    mkfifo ${postgres_pipe}
    mkfifo ${scidb_pipe}

    chmod a+rw ${postgres_pipe}
    chmod a+rw ${scidb_pipe}

    bash restart_databases.sh ${postgresql_version} ${pg_bin} ${postgresql_data_location}

    # remove data from SciDB
    iquery -aq "remove(${scidb_array});";
    iquery -aq "create array ${scidb_array} <a:int64,b:int64,val:double>[k=0:*,1000000,0];";
    # remove data from postgres 
    psql -U ${user} -d ${database} -h ${host} -p ${port} -a -c "drop table ${postgresql_table};"
    psql -U ${user} -d template1 -h ${host} -p ${port} -a -c "drop database test;";
    psql -U ${user} -d template1 -h ${host} -p ${port} -a -c "create database test;";

    bash restart_databases.sh ${postgresql_version} ${pg_bin} ${postgresql_data_location}
    psql -U ${user} -d ${database} -h ${host} -p ${port} -a -c "begin; create table ${postgresql_table} (a bigint not null, b bigint not null, val double precision not null); copy ${postgresql_table} from '${csv_file}' with (format csv, header, freeze); commit;"
    bash restart_databases.sh ${postgresql_version} ${pg_bin} ${postgresql_data_location}

    # direct binary data migration from PostgreSLQ to SciDB
    START=$(date +%s.%N)
    psql -U ${user} -d ${database} -h ${host} -p ${port} -a -c "copy ${postgresql_table} to '${postgres_pipe}' with (format binary)" &
    iquery -naq "load(${scidb_array},'${postgres_pipe}',-2,'(${format_scidb})')" &

    wait

    END=$(date +%s.%N)
    DIFF_BIN_DIRECT_MIGRATION=$(echo "$END - $START" | bc)
    echo The data migration time was ${DIFF_BIN_DIRECT_MIGRATION}

    scidb_items_bin_direct_migration=$(iquery -q "select count(*) from ${scidb_array}" | sed -n 2p | cut -d " " -f2)

    iquery -aq "remove(${scidb_array});";
    iquery -aq "create array ${scidb_array} <a:int64,b:int64,val:double>[k=0:*,1000000,0];";

    # # count # of rows
    # select="select count(*) from ${postgresql_table}"
    # echo $select
    # tuples=$(echo $select | $psql -d ${database} -P t -P format=unaligned)
    # echo "number of tuples", $tuples

    # echo Loading time to postgresql was, ${DIFF} ,seconds,number of tuples,$tuples >> log_postgres_loading.log

    bash restart_databases.sh ${postgresql_version} ${pg_bin} ${postgresql_data_location}
    # dump postgresql csv
    START=$(date +%s.%N)
    $psql -U ${user} -d ${database} -h ${host} -p ${port} -a -c "copy ${postgresql_table} to '${file_from_postgres_csv}' with (format csv, header, freeze)"
    END=$(date +%s.%N)
    DIFF_DUMP_CSV_POSTGRES=$(echo "$END - $START" | bc)    

    # clean the disk
    POSTGRES_CSV_FILE_SIZE=$(ls -lah ${file_from_postgres_csv} | cut -d" " -f5)
    rm -f ${file_from_postgres_csv}

    bash restart_databases.sh ${postgresql_version} ${pg_bin} ${postgresql_data_location}
    psql -U ${user} -d ${database} -a -c "begin; create table ${postgresql_table} (a bigint not null, b bigint not null, val double precision not null); copy ${postgresql_table} from '${csv_file}' with (format csv, header, freeze); commit;"
    bash restart_databases.sh ${postgresql_version} ${pg_bin} ${postgresql_data_location}

    # migrate data in csv format from postgresql to scidb
    START=$(date +%s.%N)
    sudo su - scidb -c "bash /home/scidb/csv-migration/from_postgres_version_to_scidb.sh ${scidb_array} ${postgresql_database} ${postgresql_user} ${postgresql_table} " # the last argument can be: ${postgresql_version}
    END=$(date +%s.%N)
    DIFF_CSV_FROM_POSTGRES_TO_SCIDB=$(echo "$END - $START" | bc)
    echo Loading time to postgresql ${DIFF_CSV_FROM_POSTGRES_TO_SCIDB}

    scidb_items_csv_migration=$(iquery -q "select count(*) from ${scidb_array}" | sed -n 2p | cut -d " " -f2)

    iquery -aq "remove(${scidb_array});";
    iquery -aq "create array ${scidb_array} <a:int64,b:int64,val:double>[k=0:*,1000000,0];";

    psql -U ${user} -d ${database} -a -c "drop table ${postgresql_table};"
    psql -U ${user} -d template1 -a -c "drop database test;";
    psql -U ${user} -d template1 -a -c "create database test;";

    # sync
    # echo drop caches; echo 3 | sudo tee /proc/sys/vm/drop_caches;
    # bash restart_databases.sh

    # START=$(date +%s.%N)
    # $psql -U ${user} -d ${database} -a -c "copy ${postgresql_table} to '${postgres_pipe}' with (format binary)" &

    # #cat ${postgres_pipe} > /tmp/postgres_file &

    # #cat ${postgres_file} > ${postgres_pipe} &

    # #cat ${postgres_pipe} > /tmp/postgres_file2

    # postgres2scidb -i ${postgres_pipe} -o ${scidb_pipe} -f${format_migrator} &

    # #cat ${scidb_pipe} > /tmp/scidb_file &

    # #cat ${scidb_file} > ${scidb_pipe}
    # iquery -naq "load(${scidb_array},'${scidb_pipe}',-2,'(${format_scidb})')" &

    # wait

    # END=$(date +%s.%N)
    # DIFF_BIN_MIGRATION=$(echo "$END - $START" | bc)
    # echo The data migration time was ${DIFF_BIN_MIGRATION}

    # scidb_items1=$(iquery -q "select count(*) from ${scidb_array}" | sed -n 2p | cut -d " " -f2)

    # iquery -aq "remove(${scidb_array});";
    # iquery -aq "create array ${scidb_array} <a:int64,b:int64,val:double>[k=0:*,1000000,0];";

    # sync
    # echo drop caches; echo 3 | sudo tee /proc/sys/vm/drop_caches;

    # # pure binary data migrator
    # START=$(date +%s.%N)
    # postgres2scidb -i ${file_from_postgres} -o ${file_to_scidb} -f${format_migrator}
    # END=$(date +%s.%N)
    # DIFF_BIN_MIGRATOR=$(echo "$END - $START" | bc)    

    # sync
    # echo drop caches; echo 3 | sudo tee /proc/sys/vm/drop_caches
    # bash restart_databases.sh

    # rm -f ${file_from_postgres}

    # # pure binary loading to scidb
    # iquery -aq "remove(${scidb_array});";
    # iquery -aq "create array ${scidb_array} <a:int64,b:int64,val:double>[k=0:*,1000000,0];";

    # START=$(date +%s.%N)
    # iquery -naq "load(${scidb_array},'${file_to_scidb}',-2,'(${format_scidb})')"
    # END=$(date +%s.%N)
    # DIFF_BIN_SCIDB_LOAD=$(echo "$END - $START" | bc)    

    # scidb_items_bin_loading=$(iquery -q "select count(*) from ${scidb_array}" | sed -n 2p | cut -d " " -f2)

    # rm -f ${file_to_scidb}

    # sync
    # echo drop caches; echo 3 | sudo tee /proc/sys/vm/drop_caches;
    # bash restart_databases.sh

    # load csv to scidb
    iquery -aq "remove(${scidb_array});";
    iquery -aq "create array ${scidb_array} <a:int64,b:int64,val:double>[k=0:*,1000000,0];";

    START=$(date +%s.%N)
    sudo su - scidb -c "bash /home/scidb/csv-migration/load_csv_to_scidb.sh ${scidb_array} ${csv_file}"
    END=$(date +%s.%N)
    DIFF_CSV_SCIDB_LOAD=$(echo "$END - $START" | bc)    

    scidb_items_csv_loading=$(iquery -q "select count(*) from ${scidb_array}" | sed -n 2p | cut -d " " -f2)
    CURRENT_TIMESTAMP=$(date -d"$CURRENT +$MINUTES minutes" '+%F_%T.%N_%Z')

    echo ${CURRENT_TIMESTAMP},csv data size,${size},GB,bin migration time,${DIFF_BIN_MIGRATION},seconds,postgres tuples,$tuples,scidb items after bin data migration,$scidb_items1,data bin migration only files postgres2scidb fromat,${DIFF_BIN_TRANSFORMATION},only bin load to scidb,${DIFF_BIN_SCIDB_LOAD},scidb items bin loading,${scidb_items_bin_loading},migrate csv from postgres to scidb,${DIFF_CSV_FROM_POSTGRES_TO_SCIDB},scidb items after csv migration,${scidb_items_csv_migration},only load bin to postgres,${DIFF_POSTGRES_BIN},POSTGRES_SIZE_BIN_LOADING,${POSTGRES_SIZE_BIN_LOADING},only dump bin postgres,${DIFF_DUMP_BIN_POSTGRES},POSTGRESQL_BIN_EXPORT_FILE_SIZE,${POSTGRESQL_BIN_EXPORT_FILE_SIZE},number of tuples postgres after binary loading,${tuples_bin},loading csv to postgresql,${DIFF_POSTGRES},POSTGRES_SIZE_AFTER_CSV_LOADING,${POSTGRES_SIZE_AFTER_CSV_LOADING},dump csv from postgres,${DIFF_DUMP_CSV_POSTGRES},postgres csv file size,${POSTGRES_CSV_FILE_SIZE},POSTGRES_CSV_LINE_NUMBER,${POSTGRES_CSV_LINE_NUMBER},only load csv to scidb,${DIFF_CSV_SCIDB_LOAD},scidb items csv loading,${scidb_items_csv_loading},binary direct migration from PostgreSQL to SciDB,${DIFF_BIN_DIRECT_MIGRATION},scidb items number after the direct migration from PostgreSQL to SciDB,${scidb_items_bin_direct_migration},DIFF_BIN_POSTGRES_EXPORT_TRANSFORMATION,${DIFF_BIN_POSTGRES_EXPORT_TRANSFORMATION},SCIDB_BIN_FILE_SIZE,${SCIDB_BIN_FILE_SIZE},DIFF_BIN_TRANSFORMATION_LOAD_BIN_SCIDB,${DIFF_BIN_TRANSFORMATION_LOAD_BIN_SCIDB},scidb_items_transformation_scidb_bin_load,${scidb_items_transformation_scidb_bin_load},DIFF_BIN_EXPORT_SCIDB,${DIFF_BIN_EXPORT_SCIDB},SCIDB_BIN_EXPORT_FILE_SIZE,${SCIDB_BIN_EXPORT_FILE_SIZE},DIFF_BIN_DIRECT_MIGRATION_TO_POSTGRES,${DIFF_BIN_DIRECT_MIGRATION_TO_POSTGRES},POSTGRES_SIZE_AFTER_DIRECT_BIN_MIGRATION_FROM_SCIDB,${POSTGRES_SIZE_AFTER_DIRECT_BIN_MIGRATION_FROM_SCIDB} >> log_parallel.log

    rm ${csv_file}
    
done

binary_loading_from_postgresql_to_scidb() {
    sync
    echo drop caches; echo 3 | sudo tee /proc/sys/vm/drop_caches;
    START=$(date +%s.%N)
    psql -U ${user} -d ${database} -h ${host} -p ${port} -a -c "begin; create table ${postgresql_table} (a bigint not null, b bigint not null, val double precision not null); copy ${postgresql_table} from '${csv_file}' with (format csv, header, freeze); commit;"
    END=$(date +%s.%N)
    DIFF_POSTGRES=$(echo "$END - $START" | bc)
    echo Loading time to postgresql ${DIFF_POSTGRES}

    POSTGRES_SIZE_AFTER_CSV_LOADING=$(du -h --max-depth=0 ${postgresql_data_location} | cut -d$'\t' -f1)
    echo POSTGRES_SIZE_AFTER_CSV_LOADING,$POSTGRES_SIZE_AFTER_CSV_LOADING

    bash restart_databases.sh ${postgresql_version} ${pg_bin} ${postgresql_data_location}
    # psql -U ${user} -d ${database} -a -c "begin; create table ${postgresql_table} (a bigint not null, b bigint not null, val double precision not null); copy ${postgresql_table} from '${csv_file}' with (format csv, header, freeze); commit;"
    # bash restart_databases.sh ${postgresql_version} ${pg_bin} ${postgresql_data_location}

    #/usr/lib/postgresql/9.3/bin/pg_ctl stop -D /mnt/ramdisk/data/PostgreSQL/
    #/usr/lib/postgresql/9.3/bin/pg_ctl start -D /mnt/ramdisk/data/PostgreSQL/

    # #non-direct binary data migration
    START=$(date +%s.%N)
    psql -U ${user} -d ${database} -a -c "copy ${postgresql_table} to '${postgres_pipe}' with (format binary, freeze)" &

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
}

clean_table_array() {
    psql -U ${user} -d ${database} -a -c "drop table ${postgresql_table};"
    psql -U ${user} -d template1 -a -c "drop database test;";
    psql -U ${user} -d template1 -a -c "create database test;";

    iquery -aq "remove(${scidb_array});";
    iquery -aq "create array ${scidb_array} <a:int64,b:int64,val:double>[k=0:*,1000000,0];";
}
