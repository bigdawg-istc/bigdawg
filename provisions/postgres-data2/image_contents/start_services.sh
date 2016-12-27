echo "STARTING POSTGRES SERVER in background"
# /usr/lib/postgresql/9.4/bin/postgres -D /var/lib/postgresql/9.4/main -p 5402 -c config_file=/etc/postgresql/9.4/main/postgresql.conf 2>&1 &
/usr/lib/postgresql/9.4/bin/postgres -D /var/lib/postgresql/9.4/main -p ${PGPORT} -c config_file=/etc/postgresql/9.4/main/postgresql.conf 2>&1 &

echo "STARTING BIGDAWG"
# java -classpath "istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar" istc.bigdawg.Main ${BDHOST}
