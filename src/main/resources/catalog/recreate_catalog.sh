# remeber where you start (which directory)
current_dir=$(pwd)

download_dir_init=$(pwd)/../../../../installation/Downloads/
pguser_init=pguser
catalog_db_init=bigdawg_catalog

catalog_resource=${1:-${current_dir}}
downloads_dir=${2:-${download_dir_init}}
port_1=${3:-5431}
postgres_bin_init=${downloads_dir}/postgres1/bin
postgres_bin=${4:-${postgres_bin_init}}
pguser=${5:-${pguser_init}}
catalog_db=${6:-${catalog_db_init}}

cd ${postgres_bin}
./psql -p ${port_1} -c "drop database if exists ${catalog_db}" -d template1 -U postgres
./psql -p ${port_1} -c "create database ${catalog_db}" -d template1 -U postgres

./psql -p ${port_1} -f ${catalog_resource}/bigdawg_ddl.sql -d ${catalog_db}
./psql -p ${port_1} -f ${catalog_resource}/inserts.sql -d ${catalog_db}

# give pguser access to the catalog
./psql -p ${port_1} -c "alter role ${pguser} with password 'test'" -d ${catalog_db}

# go back to the directory where you started
cd ${current_dir}

