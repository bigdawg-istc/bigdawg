# Used as owner of newly created databases
# pguser=pguser
# su pguser

# Create user (-s: Superuser, -d: User will be able to create databases, -e: Echo the commands)
# psql -c "CREATE USER pguser WITH SUPERUSER PASSWORD 'test';"
# createuser -s -d -e ${pguser}
# psql -c "alter role ${pguser} with password 'test'"


# if [ -d "/var/run/postgresql/9.4-main.pg_stat_tmp" ]; then
# 	mkdir /var/run/postgresql/9.4-main.pg_stat_tmp
# 	touch /var/run/postgresql/9.4-main.pg_stat_tmp/global.tmp
# fi

# /etc/init.d/postgresql start

# # catalog database
# psql -c "create database bigdawg_catalog"
# psql -f /tmp/catalog/catalog_ddl.sql -d bigdawg_catalog
# psql -f /tmp/catalog/catalog_inserts.sql -d bigdawg_catalog
# psql -f /tmp/monitor/monitor_ddl.sql -d bigdawg_catalog

# # schemas database
# psql -c "create database bigdawg_schemas"
# psql -f /tmp/schemas/mimic2_schemas_ddl.sql -d bigdawg_schemas
# psql -f /tmp/schemas/tpch_schemas_ddl.sql -d bigdawg_schemas

# # logs database
# psql -c "create database logs owner ${pguser}"
# psql -f /tmp/logs/logs_ddl.sql -d logs

# gosu postgres postgres 2>&1 &


echo "STARTING POSTGRES SERVER IN BACKGROUND"
/usr/lib/postgresql/9.4/bin/postgres -D /var/lib/postgresql/9.4/main -p 5400 -c config_file=/etc/postgresql/9.4/main/postgresql.conf 2>&1 &

echo "SLEEPING 5 SECONDS"
sleep 5

echo "STARTING BIGDAWG"
java -classpath "istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar" istc.bigdawg.Main bigdawg-postgres0





# gosu postgres -D /usr/local/pgsql/data >logfile 2>&1 &
# java -classpath "istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar" istc.bigdawg.Main