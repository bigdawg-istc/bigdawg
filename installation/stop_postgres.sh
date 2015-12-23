downloads_dir=$(pwd)/Downloads

stop() {
    postgres_version=$1
    postgres_data=${downloads_dir}/${postgres_version}/data
    postgres_bin=${downloads_dir}/${postgres_version}/bin
    cd ${postgres_bin}
    ./pg_ctl stop -D ${postgres_data}
}

stop postgres1
stop postgres2





