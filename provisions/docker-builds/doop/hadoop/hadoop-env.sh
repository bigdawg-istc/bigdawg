
# Set Hadoop-specific environment variables here.

# The only required environment variable is JAVA_HOME.  All others are
# optional.  When running a distributed configuration it is best to
# set JAVA_HOME in this file, so that it is correctly defined on
# remote nodes.

# The java implementation to use.  Required.
export JAVA_HOME=/usr
export HADOOP_HOME_WARN_SUPPRESS=1

# Hadoop home directory
export HADOOP_HOME=${HADOOP_HOME:-/usr/hdp/current/hadoop-client}

# Hadoop Configuration Directory


# Path to jsvc required by secure HDP 2.0 datanode
export JSVC_HOME=/usr/lib/bigtop-utils


# The maximum amount of heap to use, in MB. Default is 1000.
export HADOOP_HEAPSIZE="1024"

export HADOOP_NAMENODE_INIT_HEAPSIZE="-Xms1024m"

# Extra Java runtime options.  Empty by default.
export HADOOP_OPTS="-Djava.net.preferIPv4Stack=true ${HADOOP_OPTS}"

# Command specific options appended to HADOOP_OPTS when specified
export HADOOP_NAMENODE_OPTS="-server -XX:ParallelGCThreads=8 -XX:+UseConcMarkSweepGC -XX:ErrorFile=/var/log/hadoop/$USER/hs_err_pid%p.log -XX:NewSize=200m -XX:MaxNewSize=200m -XX:PermSize=128m -XX:MaxPermSize=256m -Xloggc:/var/log/hadoop/$USER/gc.log-`date +'%Y%m%d%H%M'` -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -Xms1024m -Xmx1024m -Dhadoop.security.logger=INFO,DRFAS -Dhdfs.audit.logger=INFO,DRFAAUDIT ${HADOOP_NAMENODE_OPTS}"
HADOOP_JOBTRACKER_OPTS="-server -XX:ParallelGCThreads=8 -XX:+UseConcMarkSweepGC -XX:ErrorFile=/var/log/hadoop/$USER/hs_err_pid%p.log -XX:NewSize=200m -XX:MaxNewSize=200m -Xloggc:/var/log/hadoop/$USER/gc.log-`date +'%Y%m%d%H%M'` -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -Xmx1024m -Dhadoop.security.logger=INFO,DRFAS -Dmapred.audit.logger=INFO,MRAUDIT -Dhadoop.mapreduce.jobsummary.logger=INFO,JSA ${HADOOP_JOBTRACKER_OPTS}"

HADOOP_TASKTRACKER_OPTS="-server -Xmx1024m -Dhadoop.security.logger=ERROR,console -Dmapred.audit.logger=ERROR,console ${HADOOP_TASKTRACKER_OPTS}"
export HADOOP_DATANODE_OPTS="-server -XX:ParallelGCThreads=4 -XX:+UseConcMarkSweepGC -XX:ErrorFile=/var/log/hadoop/$USER/hs_err_pid%p.log -XX:NewSize=200m -XX:MaxNewSize=200m -XX:PermSize=128m -XX:MaxPermSize=256m -Xloggc:/var/log/hadoop/$USER/gc.log-`date +'%Y%m%d%H%M'` -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -Xms1024m -Xmx1024m -Dhadoop.security.logger=INFO,DRFAS -Dhdfs.audit.logger=INFO,DRFAAUDIT ${HADOOP_DATANODE_OPTS}"
HADOOP_BALANCER_OPTS="-server -Xmx1024m ${HADOOP_BALANCER_OPTS}"

export HADOOP_SECONDARYNAMENODE_OPTS=$HADOOP_NAMENODE_OPTS

# The following applies to multiple commands (fs, dfs, fsck, distcp etc)
export HADOOP_CLIENT_OPTS="-Xmx${HADOOP_HEAPSIZE}m -XX:MaxPermSize=512m $HADOOP_CLIENT_OPTS"

# On secure datanodes, user to run the datanode as after dropping privileges
export HADOOP_SECURE_DN_USER=${HADOOP_SECURE_DN_USER:-""}

# Extra ssh options.  Empty by default.
export HADOOP_SSH_OPTS="-o ConnectTimeout=5 -o SendEnv=HADOOP_CONF_DIR"

# Where log files are stored.  $HADOOP_HOME/logs by default.
export HADOOP_LOG_DIR=/var/log/hadoop/$USER

# History server logs
export HADOOP_MAPRED_LOG_DIR=/var/log/hadoop-mapreduce/$USER

# Where log files are stored in the secure data environment.
export HADOOP_SECURE_DN_LOG_DIR=/var/log/hadoop/$HADOOP_SECURE_DN_USER

# File naming remote slave hosts.  $HADOOP_HOME/conf/slaves by default.
# export HADOOP_SLAVES=${HADOOP_HOME}/conf/slaves

# host:path where hadoop code should be rsync'd from.  Unset by default.
# export HADOOP_MASTER=master:/home/$USER/src/hadoop

# Seconds to sleep between slave commands.  Unset by default.  This
# can be useful in large clusters, where, e.g., slave rsyncs can
# otherwise arrive faster than the master can service them.
# export HADOOP_SLAVE_SLEEP=0.1

# The directory where pid files are stored. /tmp by default.
export HADOOP_PID_DIR=/var/run/hadoop/$USER
export HADOOP_SECURE_DN_PID_DIR=/var/run/hadoop/$HADOOP_SECURE_DN_USER

# History server pid
export HADOOP_MAPRED_PID_DIR=/var/run/hadoop-mapreduce/$USER

YARN_RESOURCEMANAGER_OPTS="-Dyarn.server.resourcemanager.appsummary.logger=INFO,RMSUMMARY"

# A string representing this instance of hadoop. $USER by default.
export HADOOP_IDENT_STRING=$USER

# The scheduling priority for daemon processes.  See 'man nice'.

# export HADOOP_NICENESS=10

# Use libraries from standard classpath
JAVA_JDBC_LIBS=""
#Add libraries required by mysql connector
for jarFile in `ls /usr/share/java/*mysql* 2>/dev/null`
do
  JAVA_JDBC_LIBS=${JAVA_JDBC_LIBS}:$jarFile
done
# Add libraries required by oracle connector
for jarFile in `ls /usr/share/java/*ojdbc* 2>/dev/null`
do
  JAVA_JDBC_LIBS=${JAVA_JDBC_LIBS}:$jarFile
done
# Add libraries required by nodemanager
MAPREDUCE_LIBS=/usr/hdp/current/hadoop-mapreduce-client/*
export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}${JAVA_JDBC_LIBS}:${MAPREDUCE_LIBS}

# added to the HADOOP_CLASSPATH
if [ -d "/usr/hdp/current/tez-client" ]; then
  if [ -d "/etc/tez/conf/" ]; then
    # When using versioned RPMs, the tez-client will be a symlink to the current folder of tez in HDP.
    export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:/usr/hdp/current/tez-client/*:/usr/hdp/current/tez-client/lib/*:/etc/tez/conf/
  fi
fi

# Setting path to hdfs command line
export HADOOP_LIBEXEC_DIR=/usr/hdp/current/hadoop-client/libexec

# Mostly required for hadoop 2.0
export JAVA_LIBRARY_PATH=${JAVA_LIBRARY_PATH}

export HADOOP_OPTS="-Dhdp.version=$HDP_VERSION $HADOOP_OPTS"
