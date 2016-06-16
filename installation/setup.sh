# go to bigdawgmiddle directory
# Adam Dziedzic
# logs=# select * from logs where logger like '%FromSciDBToPostgresImplementation%' and message like 'Migration result,%Bin%FULL' order by time desc;

# install updates
sudo apt-get update
sudo apt-get install -y libreadline-dev zlib1g-dev

initial_dir=$(pwd)

# use this for vagrant
ln -s ~/Downloads Downloads

# OR use this for a non-vagrant setup
mkdir Downloads

cd Downloads
downloads_dir=$(pwd)

# Download and extract postgres
wget https://ftp.postgresql.org/pub/source/v9.4.5/postgresql-9.4.5.tar.gz
tar -xf postgresql-9.4.5.tar.gz

# get catalog resource
resources=${initial_dir}/../src/main/resources/
catalog_resource=${resources}catalog/
monitor_resource=${resources}/monitor/
schemas_resource=${resources}/schemas/
catalog_db=bigdawg_catalog
database2=mimic2_copy

# database for logs
log_db=logs

# main user
pguser=pguser

# ports for postgres
port_1=5431
port_2=5430

# bin paths
postgres1_bin=${downloads_dir}/postgres1/bin
postgres2_bin=${downloads_dir}/postgres2/bin

# tpch
scale_factor=1
#TABLES_TPCH_POSTGRES1=(region part partsupp orders)
TABLES_TPCH_POSTGRES1=(region part partsupp orders nation supplier customer lineitem) # for tests
#TABLES_TPCH_POSTGRES2=(nation supplier customer lineitem)
TABLES_TPCH_POSTGRES2=(region part partsupp orders nation supplier customer lineitem) # for tests

# auxiliary function: this compiles the code, intalls and configures the database
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

# main function: download and install postgres instances (postgres1 and postgres2)
function install_postgres {
    cd ${downloads_dir}
    wget https://ftp.postgresql.org/pub/source/v9.4.5/postgresql-9.4.5.tar.gz
    tar -xf postgresql-9.4.5.tar.gz

    setDB postgres1 ${port_1}
    setDB postgres2 ${port_2}

    cd ${postgres1_bin}
    ./createuser -p ${port_1} -s -e -d postgres

    cd ${postgres2_bin}
    ./createuser -p ${port_2} -s -e -d postgres
}

# main function: this prepares the basic data in intance: postgres1
function prepare_postgres1 {
    cd ${postgres1_bin}
    ./psql -p ${port_1} -c "alter role postgres with password 'test'" -d template1

    # clean
    ./psql -p ${port_1} -c "drop database if exists ${log_db}" -d template1 -U postgres
    ./psql -p ${port_1} -c "drop database if exists mimic2" -d template1 -U postgres
    ./psql -p ${port_1} -c "drop database if exists test" -d template1 -U postgres

    ./psql -p ${port_1} -c "drop role if exists ${pguser}" -d template1
    ./createuser -p ${port_1} -s -e -d ${pguser}


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
}

# main function: this prepares the basic data in intance: postgres2
function prepare_postgres2 {
    cd ${postgres2_bin}
    ./psql -p ${port_2} -c "alter role postgres with password 'test'" -d template1

    # clean the databases
    ./psql -p ${port_2} -c "drop database if exists ${database2}" -d template1 -U postgres

    ./psql -p ${port_2} -c "drop role if exists ${pguser}" -d template1
    ./createuser -p ${port_2} -s -e -d ${pguser}
    ./psql -p ${port_2} -c "alter role ${pguser} with password 'test'" -d template1

    ./psql -p ${port_2} -c "create database ${database2} owner ${pguser}" -d template1

    # May return an error about there not being a root role. It's not an issue, so ignore
    ./psql -p ${port_2} -f ${initial_dir}/data/mimic2.pgd -U ${pguser} -d ${database2}

    # ./psql -p ${port_2} -c "create database ${database2} owner ${pguser}" -d template1
    # ./psql -p ${port_2} -c "create schema mimic2v26" -d ${database2} -U pguser

    # d_patients.sql is a legacy sample that's not even supposed to run
    # ./psql -p ${port_2} -f ${initial_dir}/../scripts/mimic2_sql/d_patients.sql -d ${database2}

    # tests
    ./psql -p ${port_2} -c "create database test owner ${pguser}" -d template1 -U postgres

    # schemas
    bash ${schemas_resource}/recreate_schemas.sh ${schemas_resource} ${downloads_dir} ${port_1} ${port_2} ${catalog_db} ${postgres1_bin} ${postgres2_bin}

}

# auxiliary function to (re)create tpch database and load the tpch arrays
function load_tables {
    # read arguments
    postgres_bin=${1}
    port=${2}
    declare -a tables=("${!3}")
    # prepare database tpch
    ${postgres_bin}/psql -p ${port} -c "drop database if exists tpch" -d template1
    ${postgres_bin}/psql -p ${port} -c "create database tpch owner ${pguser}" -d template1
    # load the tables
    for table in ${tables[*]}; do
	echo crating and loading table: ${table}
	create_table_sql=$(cat tables/${table}.sql)
	${postgres_bin}/psql -p ${port} -c "begin;${create_table_sql}; copy ${table} from '${dbgen_dir}/${table}.tbl' with (format csv, delimiter '|', freeze); commit;" -d tpch
    done;
}

# main function
function load_tpch {
# This is widely used benchmark dataset for business settings
    cd ${downloads_dir}
    mkdir -p tpch
    cd tpch
    git clone https://bitbucket.org/adam-dziedzic/tpch-generator/overview
    cd overview/dbgen
    dbgen_dir=$(pwd)
    make clean
    make
    ./dbgen -vf -s ${scale_factor}

    # postgres1
    load_tables  ${postgres1_bin} ${port_1} TABLES_TPCH_POSTGRES1[*]

    # postgres2
    load_tables ${postgres2_bin} ${port_2} TABLES_TPCH_POSTGRES2[*]
}

# main exeuction path: the function with label main are meant to be exeucted in the main path, you can comment the functions that you don't want to be executed
install_postgres
prepare_postgres1
prepare_postgres2
#load_tpch
