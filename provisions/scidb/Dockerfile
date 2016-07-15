FROM ubuntu:14.04

ADD 00_update_and_add_sources.sh /docker-entrypoint-initdb.d/

ADD 01_install_scidb.sh /docker-entrypoint-initdb.d/

ADD 02_fix_postgres_networking.sh /docker-entrypoint-initdb.d/

ADD 03_setup_scidb.sh /docker-entrypoint-initdb.d/

RUN
    cd /docker-entrypoint-initdb.d/ && \
    sh 00_update_and_add_sources.sh && \
    sh 01_install_scidb && \
    sh 02_fix_postgres_networking.sh && \
    sh 03_setup_scidb.sh