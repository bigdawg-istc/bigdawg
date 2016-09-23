FROM jboss/base-jdk:8

USER root

RUN yum install -y epel-release && \
	yum install -y curl which tar sudo openssh-server openssh-clients rsync supervisor && \
	yum update -y libselinux && \
	curl -LO https://archive.cloudera.com/cdh5/one-click-install/redhat/7/x86_64/cloudera-cdh-5-0.x86_64.rpm && \
	yum -y --nogpgcheck localinstall cloudera-cdh-5-0.x86_64.rpm && \
	rpm --import http://archive.cloudera.com/cdh5/redhat/7/x86_64/cdh/RPM-GPG-KEY-cloudera && \
	yum install -y hadoop-conf-pseudo && \
	curl -s http://apache.claz.org/accumulo/1.6.5/accumulo-1.6.5-bin.tar.gz | tar -xz -C /usr/lib && \
	ln -s /usr/lib/accumulo-1.6.5 /usr/lib/accumulo && \
	yum clean all
	
#Users, Groups and Directories
RUN mkdir -p /var/run/sshd && mkdir -p /var/log/supervisor /var/log/accumulo && \
	groupadd -r supervisor -g 3000 && chmod 775 /var/log/supervisor && chown -R root:supervisor /var/log/supervisor && \
	chmod 775 /var/run/sshd  && chown -R root:hadoop /var/run/sshd && \
	groupadd -r accumulo -g 3500 && useradd -u 2000 -r -g accumulo -G hadoop,zookeeper -m -d /home/accumulo -s /bin/nologin -c "Accumulo user" accumulo && \
	chmod 775 /var/log/accumulo && chown -R accumulo:accumulo /var/log/accumulo

# passwordless ssh
RUN ssh-keygen -q -N "" -t dsa -f /etc/ssh/ssh_host_dsa_key && \
	ssh-keygen -q -N "" -t rsa -f /etc/ssh/ssh_host_rsa_key && \
	ssh-keygen -q -N "" -t rsa -f /root/.ssh/id_rsa && \
	cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys
	
#Hadoop, Zookeeper and Accumulo Environment Variables
ENV HADOOP_HOME=/usr/lib/hadoop HADOOP_PREFIX=/usr/lib/hadoop \
	HADOOP_LIBEXEC_DIR=/usr/lib/hadoop/libexec HADOOP_COMMON_HOME=/usr/lib/hadoop HADOOP_HDFS_HOME=/usr/lib/hadoop-hdfs \
	HADOOP_MAPRED_HOME=/usr/lib/hadoop-mapreduce HADOOP_YARN_HOME=/usr/lib/hadoop-yarn \
	HADOOP_CONF_DIR=/usr/lib/hadoop/etc/hadoop YARN_CONF_DIR=/usr/lib/hadoop-yarn/etc/hadoop \
	HADOOP_LOG_DIR=/var/log/hadoop-hdfs HADOOP_MAPRED_LOG_DIR=/var/log/hadoop-mapreduce YARN_LOG_DIR=/var/log/hadoop-yarn \
	ACCUMULO_SETUP_DIR=/etc/accumulo ZOOKEEPER_HOME=/usr/lib/zookeeper ZOO_LOG_DIR=/var/log/zookeeper ACCUMULO_HOME=/usr/lib/accumulo \
	ACCUMULO_LOG_DIR=/var/log/accumulo

ENV PATH=$PATH:$HADOOP_HOME/bin:$JAVA_HOME/bin:$ACCUMULO_HOME/bin:$ZOOKEEPER_HOME/bin

ADD hadoop/ssh_config /root/.ssh/config
ADD accumulo/*.sh $ACCUMULO_SETUP_DIR/
ADD startup.sh $ACCUMULO_SETUP_DIR/

ADD accumulo/conf/* $ACCUMULO_SETUP_DIR/conf/

RUN chown root:root /root/.ssh/config && \	 
	chmod 600 /root/.ssh/config	&& \
	chmod 700 $ACCUMULO_SETUP_DIR/*.sh && \
	chown root:root $ACCUMULO_SETUP_DIR/*.sh
	
RUN $ACCUMULO_SETUP_DIR/setup_hadoop.sh && \
	$ACCUMULO_SETUP_DIR/setup_zookeeper.sh && \
	$ACCUMULO_SETUP_DIR/setup_accumulo.sh

#Replace Hadoop and Zookeeper Configuration
ADD hadoop/conf/* $HADOOP_CONF_DIR/
ADD zookeeper/* $ZOOKEEPER_HOME/conf/

RUN $HADOOP_HOME/etc/hadoop/hadoop-env.sh && \
	sed "s/HOSTNAME/$HOSTNAME/g" $HADOOP_CONF_DIR/core-site.xml.template > $HADOOP_CONF_DIR/core-site.xml
USER hdfs
RUN $HADOOP_HDFS_HOME/bin/hdfs namenode -format

User root

###PORTS	
## Hdfs ports
EXPOSE 50010 50020 50070 50075 50090 8020 9000
## Mapred ports
EXPOSE 19888
## Yarn ports
EXPOSE 8030 8031 8032 8033 8040 8042 8088
## Other ports
EXPOSE 49707 2122
## Zookeeper Ports
EXPOSE 2181
## Accumulo Ports
EXPOSE 4560 9997 9999 12234 50091 50095

#Supervisord for managing the services
ADD supervisord/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

CMD ["/etc/accumulo/startup.sh", "-d"]
