#!/bin/bash
#  this re-formats if necessary (if the hostname has changed since the last format)

if [ -f /etc/hadoop/conf/core-site.xml ]
then
  namenode_host=$(grep 8020 /etc/hadoop/conf/core-site.xml|sed 's%.*hdfs://%%'|awk -F':' '{print $1}')
fi

if [ "${namenode_host}" != "$(hostname)" ]
then
  supervisorctl stop all
  /etc/hadoop/conf.templates/format-namenode.sh
fi

supervisorctl start all
ps axo pid,args|grep -v ps
