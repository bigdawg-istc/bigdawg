FROM ubuntu:14.04

ADD 00_update_and_add_sources.sh /docker-entrypoint-initdb.d/

ADD 01_install_scidb.sh /docker-entrypoint-initdb.d/

ADD 02_fix_postgres_networking.sh /docker-entrypoint-initdb.d/

ADD 03_setup_scidb.sh /docker-entrypoint-initdb.d/

RUN cd /docker-entrypoint-initdb.d/ && \
    sh 00_update_and_add_sources.sh

RUN cd /docker-entrypoint-initdb.d/ && \
    sh 01_install_scidb.sh && \
    sh 02_fix_postgres_networking.sh

USER scidb
    echo '#!/usr/bin/expect' >> setupscidb.exp
    echo 'set timeout 20' >> setupscidb.exp
    echo "spawn \"scidb-prepare-db.sh\"" >> setupscidb.exp
    echo "expect \"PostgreSQL administrator login \(postgres by default\)\" {send \"\r\"}" >> setupscidb.exp
    echo "expect \"ident authentification to local server with sudo will be used\):\" {send \"\r\"}" >> setupscidb.exp
    echo "expect \"Enter system catalog owner login:\" {send \"scidb\r\"}" >> setupscidb.exp
    echo "expect \"Enter system catalog owner password:\" {send \"mypassw\r\"}" >> setupscidb.exp
    echo "expect \"Enter catalog database name:\" {send \"mydb\r\"}" >> setupscidb.exp
    echo "interact" >> setupscidb.exp
    chmod 750 setupscidb.exp
    ./setupscidb.exp
    echo "====== scidb initall single_server ======"
    yes | python /opt/scidb/14.12/bin/scidb.py -m scidb initall single_server
    echo "====== startall single_server ======"
    python /opt/scidb/14.12/bin/scidb.py startall single_server
    # sh 03_setup_scidb.sh