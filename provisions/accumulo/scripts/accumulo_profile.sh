if [ -x /usr/lib/accumulo/bin/accumulo ]
then
  export ACCUMULO_HOME=/usr/lib/accumulo
  export PATH=$PATH:$ACCUMULO_HOME/bin
fi
