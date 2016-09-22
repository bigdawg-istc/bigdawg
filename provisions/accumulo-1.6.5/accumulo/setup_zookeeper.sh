#!/bin/bash

chmod 775 /var/log/zookeeper
mkdir -p /var/data/zookeeper
chown -R zookeeper:zookeeper /var/data/zookeeper
chmod 775 /var/data/zookeeper