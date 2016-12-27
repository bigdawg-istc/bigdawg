FROM centos:7
MAINTAINER Steffen Roegner "steffen.roegner@gmail.com"
USER root

ENV JAVA_HOME=/usr
ENV HADOOP_HOME=/usr/hdp/current/hadoop-client
ENV HADOOP_HDFS_HOME=/usr/hdp/current/hadoop-hdfs-client
ENV HADOOP_MAPRED_HOME=/usr/hdp/current/hadoop-mapreduce-client
ENV HADOOP_YARN_HOME=/usr/hdp/current/hadoop-yarn-client
ENV HADOOP_LIBEXEC_DIR=/usr/hdp/current/hadoop-client/libexec

ENV REFRESHED_AT 2016-04-21

RUN rpm -ivh https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm; \
    yum -y -q upgrade; \
    yum -y install python-pip snappy lzo rsync which tar bind-utils java-1.7.0-openjdk-devel unzip; \
    yum clean all; \
    pip install supervisor 

RUN curl -L http://public-repo-1.hortonworks.com/HDP/centos7/2.x/updates/2.4.0.0/hdp.repo -o /etc/yum.repos.d/hdp.repo; \
    yum -y install hadoop hadoop-hdfs hadoop-libhdfs hadoop-yarn hadoop-mapreduce hadoop-client zookeeper 
    
RUN mkdir -p /data1/hdfs /data1/mapred /data1/yarn /var/log/hadoop /var/log/hadoop-yarn /var/log/supervisor /var/lib/zookeeper; \
    chown hdfs.hadoop /data1/hdfs && \
    chown mapred.hadoop /data1/mapred && \
    chown yarn.hadoop /data1/yarn; \
    chown zookeeper.hadoop /var/lib/zookeeper; \
    chmod 775 /var/log/hadoop; chgrp hadoop /var/log/hadoop

COPY supervisord.conf /etc/
COPY hadoop /etc/hadoop/conf
COPY conf.zk/zookeeper-env.sh /etc/zookeeper/conf/
COPY conf.zk/zoo.cfg /etc/zookeeper/conf/
COPY check_hadoop.sh /usr/local/sbin/
COPY bootstrap-node.conf /etc/supervisor/conf.d/
COPY bootstrap-node.sh /usr/local/sbin/
COPY hadoop-group.conf /etc/supervisor/conf.d/

USER hdfs
RUN HADOOP_ROOT_LOGGER="WARN,console" /usr/bin/hdfs namenode -format

USER root
VOLUME /etc/hadoop/conf
