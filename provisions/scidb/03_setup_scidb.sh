echo '#!/usr/bin/expect' >> setupscidb.exp
echo 'set timeout 20' >> setupscidb.exp
echo "spawn \"/opt/scidb/14.12/bin/scidb-prepare-db.sh\"" >> setupscidb.exp
echo "expect \"PostgreSQL administrator login \(postgres by default\)\" {send \"\r\"}" >> setupscidb.exp
echo "expect \"ident authentification to local server with sudo will be used\):\" {send \"\r\"}" >> setupscidb.exp
echo "expect \"Enter system catalog owner login:\" {send \"scidb\r\"}" >> setupscidb.exp
echo "expect \"Enter system catalog owner password:\" {send \"mypassw\r\"}" >> setupscidb.exp
echo "expect \"Enter catalog database name:\" {send \"mydb\r\"}" >> setupscidb.exp
echo "interact" >> setupscidb.exp
chmod 750 setupscidb.exp
./setupscidb.exp
yes | python /opt/scidb/14.12/bin/scidb.py -m scidb initall single_server
