# go to bigdawgmiddle directory

initial_dir=$(pwd)
mkdir Downloads
cd Downloads
downloads_dir=$(pwd)
wget https://ftp.postgresql.org/pub/source/v9.4.5/postgresql-9.4.5.tar.gz
tar -xf postgresql-9.4.5.tar.gz
cd postgresql-9.4.5

./configure --prefix=${downloads_dir}/postgres1
make
make install

./configure --prefix=${downloads_dir}/postgres2
make
make install

port_1=5432

sudo su postgres
psql -p ${port_1} -c "drop database bigdawg_catalog";
psql -p ${port_1} -c "create database bigdawg_catalog";

psql -p ${port_1} -f ../src/main/resources/bigdawg_ddl.sql

psql -p ${port_1} -c "alter role postgres with password 'test'";

createuser -p ${port_1} -s -e -d pguser
psql -p ${port_1} -c "alter role pguser with password 'test'";
psql -p ${port_1} -c "create database mimic2_copy owner pguser";

psql -p ${port_1} -f ../src/main/resources/bigdawg_catalog_data.sql -d bigdawg_catalog


