current_dir=$(pwd)
download_dir_init=$(pwd)/../../../../installation/Downloads/

schemas_resource=${1:-${current_dir}}
downloads_dir=${2:-${download_dir_init}}
port_1=${3:-5431}
port_2=${4:-5430}

echo downloads_dir: $downloads_dir
postgres1_bin_init=${downloads_dir}/postgres1/bin
postgres1_bin=${5:-${postgres1_bin_init}}

postgres2_bin_init=${downloads_dir}/postgres2/bin
postgres2_bin=${6:-${postgres2_bin_init}}

database=bigdawg_schemas

cd ${postgres1_bin}
echo postgres1_bin $(pwd)
./psql -p ${port_1} -c "drop database if exists ${database}" -d template1
./psql -p ${port_1} -c "create database ${database}" -d template1

./psql -p ${port_1} -f ${schemas_resource}/bigdawg_schemas_setup.sql -d ${database}
./psql -p ${port_1} -f ${schemas_resource}/plain.sql -d ${database}
./psql -p ${port_1} -f ${schemas_resource}/insert_into_patients.sql -d ${database}


cd ${postgres2_bin}
./psql -p ${port_2} -c "drop database if exists ${database}" -d template1
./psql -p ${port_2} -c "create database ${database}" -d template1

./psql -p ${port_2} -f ${schemas_resource}/bigdawg_schemas_setup.sql -d ${database}
./psql -p ${port_2} -f ${schemas_resource}/plain.sql -d ${database}
./psql -p ${port_1} -f ${schemas_resource}/insert_into_ailment.sql -d ${database}


cd ${current_dir}

