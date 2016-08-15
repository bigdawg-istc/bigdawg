#!/bin/bash
cp -r $ACCUMULO_HOME/conf/* $ACCUMULO_SETUP_DIR/conf/
chmod 755 $ACCUMULO_SETUP_DIR/*.sh

rm -rf $ACCUMULO_HOME/conf/
ln -s $ACCUMULO_SETUP_DIR/conf $ACCUMULO_HOME