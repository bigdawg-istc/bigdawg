FROM postgres:9.4.5
ADD mimic2.pgd /tmp/mimic2.pgd

#ADD 00_require_password_for_remote_connections.sh /docker-entrypoint-initdb.d/

# https://hub.docker.com/_/postgres/
# If you would like to do additional initialization in an image derived from this one, add one or more *.sql or *.sh
# scripts under /docker-entrypoint-initdb.d (creating the directory if necessary). After the entrypoint calls initdb to
# create the default postgres user and database, it will run any *.sql files and source any *.sh scripts found in that
# directory to do further initialization before starting the service.