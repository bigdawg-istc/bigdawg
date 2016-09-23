#!/bin/bash

#Stop Hadoop Services
echo "Stopping Map Reduce Job History Daemon History Server with configuration in: " $HADOOP_CONF_DIR 
su -s /bin/bash mapred -c '$HADOOP_MAPRED_HOME/sbin/mr-jobhistory-daemon.sh --config $HADOOP_CONF_DIR stop historyserver'

echo "Stopping Yarn Daemon Node Manager with configuration in: " $YARN_CONF_DIR 
su -s /bin/bash yarn -c '$HADOOP_YARN_HOME/sbin/yarn-daemon.sh --config $YARN_CONF_DIR stop nodemanager'

echo "Stopping Yarn Daemon Resource Manager with configuration in: " $YARN_CONF_DIR
su -s /bin/bash yarn -c '$HADOOP_YARN_HOME/sbin/yarn-daemon.sh --config $YARN_CONF_DIR stop resourcemanager'

echo "Stopping Hadoop Daemon Secondary Name Node with configuration in: " $HADOOP_CONF_DIR 
su -s /bin/bash hdfs -c '$HADOOP_COMMON_HOME/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR stop secondarynamenode'

echo "Stopping Hadoop Daemon Data Node with configuration in: " $HADOOP_CONF_DIR 
su -s /bin/bash hdfs -c '$HADOOP_COMMON_HOME/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR stop datanode'

echo "Stopping Hadoop Daemon Name Node with configuration in: " $HADOOP_CONF_DIR 
su -s /bin/bash hdfs -c '$HADOOP_COMMON_HOME/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR stop namenode'
