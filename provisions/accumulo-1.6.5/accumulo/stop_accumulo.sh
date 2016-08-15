#!/bin/bash

#Stop Accumulo
su -s /bin/bash accumulo -c '$ACCUMULO_HOME/bin/stop-all.sh'
