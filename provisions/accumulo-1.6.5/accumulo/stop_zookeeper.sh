#!/bin/bash

#Stop Zookeeper
su -s /bin/bash zookeeper -c '$ZOOKEEPER_HOME/bin/zkServer.sh stop'
