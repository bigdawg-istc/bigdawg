#!/bin/bash

sed "s/HOSTNAME/$HOSTNAME/g" $ACCUMULO_HOME/conf/accumulo-site-template.xml > $ACCUMULO_HOME/conf/accumulo-site.xml
sed "s/HOSTNAME/$HOSTNAME/g" $ACCUMULO_HOME/conf/client.conf.template > $ACCUMULO_HOME/conf/client.conf

echo $HOSTNAME > $ACCUMULO_HOME/conf/gc
echo $HOSTNAME > $ACCUMULO_HOME/conf/masters
echo $HOSTNAME > $ACCUMULO_HOME/conf/monitor
echo $HOSTNAME > $ACCUMULO_HOME/conf/slaves
echo $HOSTNAME > $ACCUMULO_HOME/conf/tracers

#Start Accumulo
su -s /bin/bash accumulo -c '$ACCUMULO_HOME/bin/start-all.sh'
