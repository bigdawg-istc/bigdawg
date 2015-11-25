bash $ZOOKEEPER_HOME/bin/zkServer.sh start
bash $HADOOP_HOME/sbin/start-dfs.sh
hadoop fs -rm -R hdfs://localhost:9000/accumulo
bash $ACCUMULO_HOME/bin/accumulo init --instance-name adam --password mypassw
bash $ACCUMULO_HOME/bin/start-all.sh
# jps for hadoop check - shows the running services, it should give you at least: DataNode, NameNode, SecondaryNameNode

$ACCUMULO_HOME/bin/accumulo shell -u root
#pass=mypassw
