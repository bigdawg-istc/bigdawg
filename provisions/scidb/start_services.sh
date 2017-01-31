# Modify etc/hosts so scidb starts. 
python edit_etchosts.py

# start postgres, ssh, scidb
service postgresql restart
service ssh restart
cd /opt/scidb/14.12/bin
echo y | ./scidb.py initall single_server
./scidb.py startall single_server

echo "Scidb is ready..."

# Keep the container active and running
tail -f /dev/null