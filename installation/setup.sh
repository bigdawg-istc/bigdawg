# go to bigdawgmiddle directory
# Adam Dziedzic

initial_dir=$(pwd)
mkdir Downloads
cd Downloads
downloads_dir=$(pwd)
wget https://ftp.postgresql.org/pub/source/v9.4.5/postgresql-9.4.5.tar.gz
tar -xf postgresql-9.4.5.tar.gz

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

setDB postgres1 ${port_1}
setDB postgres2 ${port_2}

postgres_bin=${downloads_dir}/postgres1/bin
cd ${postgres_bin}

# get catalog resource 
catalog_resource=../src/main/resources/catalog/
current_dir=$(pwd)
cd $catalog_resource
catalog_resource=$(pwd)
cd ${current_dir}
bash ${catalog_resource}/recreate_catalog.sh ${catalog_resource} ${downloads_dir} ${port_1} ${postgres_bin}
cd ${current_dir}

./createuser -p ${port_1} -s -e -d postgres
./psql -p ${port_1} -c "alter role postgres with password 'test'" -d ${database}
./psql -p ${port_1} -f ${initial_dir}/../src/main/resources/catalog/inserts.sql -d ${database}

./createuser -p ${port_1} -s -e -d pguser
./psql -p ${port_1} -c "alter role pguser with password 'test'" -d ${database}

./psql -p ${port_1} -c "create database mimic2 owner pguser" -d ${database}
./psql -p ${port_1} -f ${initial_dir}/data/mimic2.pgd -U pguser -d mimic2

log_db=logs
./psql -p ${port_1} -c "create database ${log_db} owner pguser" -d template1
./psql -p ${port_1} -f ${initial_dir}/../scripts/create_log_table_database_logs.sql -d ${log_db}

postgres2_bin=${downloads_dir}/postgres2/bin
database2=mimic2_copy
cd ${postgres2_bin}
./createuser -p ${port_2} -s -e -d pguser
./psql -p ${port_2} -c "alter role pguser with password 'test'" -d template1
./psql -p ${port_2} -c "create database ${database2} owner pguser" -d template1
./psql -p ${port_2} -c "create schema mimic2v26" -d ${database2}
./psql -p ${port_2} -f ${initial_dir}/../scripts/mimic2_sql/d_patients.sql -d ${database2}




