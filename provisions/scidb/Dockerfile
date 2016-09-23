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

RUN mv /docker-entrypoint-initdb.d/03_setup_scidb.sh /home/scidb/03_setup_scidb.sh && \
    chown scidb /home/scidb/03_setup_scidb.sh && \
    chmod 755 /home/scidb/03_setup_scidb.sh

RUN service ssh start && \
    /etc/init.d/postgresql restart && \
    cd /home/scidb && \
    su scidb 03_setup_scidb.sh

COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

EXPOSE 22
CMD ["/usr/bin/supervisord"]
