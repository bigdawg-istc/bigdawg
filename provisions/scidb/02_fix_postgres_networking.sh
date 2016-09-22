sed -i '$ d' /etc/postgresql/9.3/main/pg_hba.conf
echo "host    all             all             ::1/128                 md5" >> /etc/postgresql/9.3/main/pg_hba.conf
/etc/init.d/postgresql  restart

# USER scidb
#     cd /docker-entrypoint-initdb.d/ && \
#     sh 03_setup_scidb.sh