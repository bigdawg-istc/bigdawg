# go to bigdawgmiddle directory
# Adam Dziedzic

initial_dir=$(pwd)
mkdir Downloads
cd Downloads
downloads_dir=$(pwd)
wget https://ftp.postgresql.org/pub/source/v9.4.5/postgresql-9.4.5.tar.gz
tar -xf postgresql-9.4.5.tar.gz

# get catalog resource 
resources=${initial_dir}/../src/main/resources/
catalog_resource=${resources}catalog/
monitor_resource=${resources}/monitor/
schemas_resource=${resources}/schemas/

# database for logs
log_db=logs

# main user
pguser=pguser

function setDB {
    postgres_version=$1
    port=$2
    init_dir=$(pwd)
    cd ${downloads_dir}
    cd postgresql-9.4.5

    ./configure --prefix=${downloads_dir}/${postgres_version}
    make
    make install

    postgres_data=${downloads_dir}/${postgres_version}/data
    mkdir -p ${postgres_data}

    postgres_bin=${downloads_dir}/${postgres_version}/bin
    cd ${postgres_bin}
    ./initdb -D ${postgres_data}
    cp ${postgres_data}/postgresql.conf ${postgres_data}/postgresql.conf.backup
    python ${initial_dir}/change_port.py -f ${postgres_data}/postgresql.conf -p ${port}
    ./pg_ctl -w start -l postgres.log -D ${postgres_data}
    cd ${init_dir}
}
port_1=5431
port_2=5430

#setDB postgres1 ${port_1}
#setDB postgres2 ${port_2}

postgres1_bin=${downloads_dir}/postgres1/bin
cd ${postgres1_bin}
./createuser -p ${port_1} -s -e -d postgres
./psql -p ${port_1} -c "alter role postgres with password 'test'" -d template1

# clean
./psql -p ${port_1} -c "drop database if exists ${log_db}" -d template1 -U postgres
./psql -p ${port_1} -c "drop database if exists mimic2" -d template1 -U postgres
./psql -p ${port_1} -c "drop database if exists test" -d template1 -U postgres

./psql -p ${port_1} -c "drop role if exists ${pguser}" -d template1
./createuser -p ${port_1} -s -e -d ${pguser}

catalog_db=bigdawg_catalog
bash ${catalog_resource}/recreate_catalog.sh ${catalog_resource} ${downloads_dir} ${port_1} ${postgres1_bin} ${pguser} ${catalog_db}

./psql -p ${port_1} -c "create database mimic2 owner ${pguser}" -d template1
./psql -p ${port_1} -f ${initial_dir}/data/mimic2.pgd -U ${pguser} -d mimic2

# tables for monitor
./psql -p ${port_1} -f ${monitor_resource}/monitor.sql -d ${catalog_db}

# log_db
./psql -p ${port_1} -c "create database ${log_db} owner ${pguser}" -d template1
./psql -p ${port_1} -f ${initial_dir}/../src/main/resources/create_logs_table.sql -d ${log_db}

# tests
./psql -p ${port_1} -c "create database test owner ${pguser}" -d template1 -U postgres

postgres2_bin=${downloads_dir}/postgres2/bin
database2=mimic2_copy
cd ${postgres2_bin}
./createuser -p ${port_2} -s -e -d postgres
./psql -p ${port_2} -c "alter role postgres with password 'test'" -d template1

# clean the databases
./psql -p ${port_2} -c "drop database if exists ${database2}" -d template1 -U postgres

./psql -p ${port_2} -c "drop role if exists ${pguser}" -d template1
./createuser -p ${port_2} -s -e -d ${pguser}
./psql -p ${port_2} -c "alter role ${pguser} with password 'test'" -d template1

./psql -p ${port_2} -c "create database ${database2} owner ${pguser}" -d template1
./psql -p ${port_2} -c "create schema mimic2v26" -d ${database2} -U pguser
./psql -p ${port_2} -f ${initial_dir}/../scripts/mimic2_sql/d_patients.sql -d ${database2}

# schemas
bash ${schemas_resource}/recreate_schemas.sh ${schemas_resource} ${downloads_dir} ${port_1} ${port_2} ${catalog_db} ${postgres1_bin} ${postgres2_bin}






