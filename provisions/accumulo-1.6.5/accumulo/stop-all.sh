#!/bin/bash

#Stop Accumulo
$ACCUMULO_SETUP_DIR/stop_accumulo.sh

#Stop Hadoop Services
$ACCUMULO_SETUP_DIR/stop_hadoop.sh

#Stop Zookeeper
$ACCUMULO_SETUP_DIR/stop_zookeeper.sh
