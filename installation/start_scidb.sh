sudo /etc/init.d/postgresql stop
sudo /etc/init.d/postgresql start
sleep 10
sudo su - scidb -c "/opt/scidb/14.12/bin/scidb.py stop_all single_server && /opt/scidb/14.12/bin/scidb.py start_all single_server"
