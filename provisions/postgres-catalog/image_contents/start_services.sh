# Start postgres
# Port is configured by `ENV PGPORT` in Dockerfile or specify -e option when using `docker run`
echo "STARTING POSTGRES SERVER IN BACKGROUND"
/usr/lib/postgresql/9.4/bin/postgres -D /var/lib/postgresql/9.4/main -p ${PGPORT} -c config_file=/etc/postgresql/9.4/main/postgresql.conf 2>&1 &

echo "STARTING BIGDAWG"
java -classpath "istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar" istc.bigdawg.Main ${BDHOST}