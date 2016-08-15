#!/bin/bash

rm /tmp/*.pid

#Start Supervisor for SSHD
/usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf

#Start Zookeeper
$ACCUMULO_SETUP_DIR/start_zookeeper.sh

#Start Hadoop Services
$ACCUMULO_SETUP_DIR/start_hadoop.sh

su -s /bin/bash hdfs -c '$HADOOP_HDFS_HOME/bin/hdfs dfsadmin -safemode wait'

#Start Accumulo
$ACCUMULO_SETUP_DIR/start_accumulo.sh
