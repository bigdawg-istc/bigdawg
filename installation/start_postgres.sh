downloads_dir=$(pwd)/Downloads

start() {
    postgres_version=$1
    postgres_data=${downloads_dir}/${postgres_version}/data
    postgres_bin=${downloads_dir}/${postgres_version}/bin
    cd ${postgres_bin}
    ./pg_ctl -w start -l postgres.log -D ${postgres_data}
}

start postgres1
start postgres2





