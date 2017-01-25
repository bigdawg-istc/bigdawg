# Start postgres 
# (port configured by PGPORT environment variable. Set with `ENV PGPORT` in Dockerfile or -e option when using `docker run`)
echo "STARTING POSTGRES SERVER IN BACKGROUND"
/usr/lib/postgresql/9.4/bin/postgres -D /var/lib/postgresql/9.4/main -p ${PGPORT} -c config_file=/etc/postgresql/9.4/main/postgresql.conf 2>&1 &

# Keep the container active and running
tail -f /dev/null