current_dir=$(pwd)
download_dir_init=$(pwd)/../../../../installation/Downloads/

catalog_resource=${1:-${current_dir}}
downloads_dir=${2:-${download_dir_init}}
port_1=${3:-5431}

echo downloads_dir: $downloads_dir
postgres_bin_init=${downloads_dir}/postgres1/bin
postgres_bin=${4:-${postgres_bin_init}}

cd ${postgres_bin}
database=bigdawg_catalog
./psql -p ${port_1} -c "drop database if exists ${database}" -d template1
./psql -p ${port_1} -c "create database ${database}" -d template1

./psql -p ${port_1} -f ${catalog_resource}/bigdawg_ddl.sql -d ${database}
./psql -p ${port_1} -f ${catalog_resource}/inserts.sql -d ${database}

cd ${current_dir}
