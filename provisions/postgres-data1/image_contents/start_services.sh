echo "STARTING POSTGRES SERVER IN BACKGROUND"
# Use this command for defaults:
# /etc/init.d/postgresql start  

# Use this command to control the port
/usr/lib/postgresql/9.4/bin/postgres -D /var/lib/postgresql/9.4/main -p ${PGPORT} -c config_file=/etc/postgresql/9.4/main/postgresql.conf 2>&1 &

# echo "STARTING BIGDAWG"
# cd /image_contents
# java -classpath "istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar" istc.bigdawg.Main ${BDHOST}

tail -f /dev/null