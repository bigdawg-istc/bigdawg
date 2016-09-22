#!/bin/bash

export USER=`whoami`

sed "s/HOSTNAME/$HOSTNAME/g" $ACCUMULO_HOME/conf/accumulo-site-template.xml > $ACCUMULO_HOME/conf/accumulo-site.xml
sed "s/HOSTNAME/$HOSTNAME/g" $ACCUMULO_HOME/conf/client.conf.template > $ACCUMULO_HOME/conf/client.conf
sed "s/HOSTNAME/$HOSTNAME/g" $HADOOP_CONF_DIR/core-site.xml.template > $HADOOP_CONF_DIR/core-site.xml

echo $HOSTNAME > $ACCUMULO_HOME/conf/gc
echo $HOSTNAME > $ACCUMULO_HOME/conf/masters
echo $HOSTNAME > $ACCUMULO_HOME/conf/monitor
echo $HOSTNAME > $ACCUMULO_HOME/conf/slaves
echo $HOSTNAME > $ACCUMULO_HOME/conf/tracers

#Set hadoop env
$HADOOP_HOME/etc/hadoop/hadoop-env.sh
su -s /bin/bash hdfs -c '$HADOOP_HDFS_HOME/bin/hdfs namenode -format'

rm /tmp/*.pid

#Start Supervisor for SSHD
/usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf

#Start Zookeeper
$ACCUMULO_SETUP_DIR/start_zookeeper.sh

#Start Hadoop Services
#Start Name Node
$ACCUMULO_SETUP_DIR/start_hadoop.sh

su -s /bin/bash hdfs -c '$HADOOP_HDFS_HOME/bin/hdfs dfsadmin -safemode wait'

#Initiate Accumulo
su -s /bin/bash accumulo -c '$ACCUMULO_HOME/bin/accumulo init --instance-name accumulo --password secret'

#Reset the hadoop env
$HADOOP_HOME/etc/hadoop/hadoop-env.sh

#Stop Hadoop Services
$ACCUMULO_SETUP_DIR/stop_hadoop.sh

#Stop Zookeeper
$ACCUMULO_SETUP_DIR/stop_zookeeper.sh
