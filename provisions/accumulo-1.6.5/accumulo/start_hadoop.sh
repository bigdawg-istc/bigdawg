#!/bin/bash

sed "s/HOSTNAME/$HOSTNAME/g" $HADOOP_CONF_DIR/core-site.xml.template > $HADOOP_CONF_DIR/core-site.xml

#Start Hadoop Services
echo "Starting Hadoop Daemon Name Node with configuration in: " $HADOOP_CONF_DIR 
su -s /bin/bash hdfs -c '$HADOOP_COMMON_HOME/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR start namenode'

echo "Starting Hadoop Daemon Data Node with configuration in: " $HADOOP_CONF_DIR 
su -s /bin/bash hdfs -c '$HADOOP_COMMON_HOME/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR start datanode'

echo "Starting Hadoop Daemon Secondary Name Node with configuration in: " $HADOOP_CONF_DIR 
su -s /bin/bash hdfs -c '$HADOOP_COMMON_HOME/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR start secondarynamenode'

echo "Starting Yarn Daemon Resource Manager with configuration in: " $YARN_CONF_DIR 
su -s /bin/bash yarn -c '$HADOOP_YARN_HOME/sbin/yarn-daemon.sh --config $YARN_CONF_DIR start resourcemanager'

echo "Starting Yarn Daemon Node Manager with configuration in: " $YARN_CONF_DIR 
su -s /bin/bash yarn -c '$HADOOP_YARN_HOME/sbin/yarn-daemon.sh --config $YARN_CONF_DIR start nodemanager'

echo "Starting Map Reduce Job History Daemon History Server with configuration in: " $HADOOP_CONF_DIR 
su -s /bin/bash mapred -c '$HADOOP_MAPRED_HOME/sbin/mr-jobhistory-daemon.sh --config $HADOOP_CONF_DIR start historyserver'
