FROM postgres:9.4.5

ADD mimic2.pgd /tmp/mimic2.pgd

ADD 00_require_password_for_remote_connections.sh /docker-entrypoint-initdb.d/
