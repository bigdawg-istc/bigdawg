FROM alpine:3.1
MAINTAINER Steffen Roegner "steffen.roegner@gmail.com"
USER root

ENV JAVA_HOME=/usr
ENV HADOOP_HOME=/usr/hdp/current/hadoop-client
ENV HADOOP_HDFS_HOME=/usr/hdp/current/hadoop-hdfs-client
ENV HADOOP_MAPRED_HOME=/usr/hdp/current/hadoop-mapreduce-client
ENV HADOOP_YARN_HOME=/usr/hdp/current/hadoop-yarn-client
ENV HADOOP_LIBEXEC_DIR=/usr/hdp/current/hadoop-client/libexec

ENV REFRESHED_AT 2016-03-29

RUN apk add --update curl py-pip lzo rsync zip openjdk7-jre-base; \
    rm -rf /var/cache/apk/*; \
    pip install supervisor 

RUN curl -L http://public-repo-1.hortonworks.com/HDP/centos6/2.x/updates/2.3.4.0/tars/hadoop-2.7.1.2.3.4.0-3485.tar.gz | tar xz -C /usr/lib
RUN curl -L http://apache.osuosl.org/zookeeper/stable/zookeeper-3.4.6.tar.gz | tar xz -C /usr/lib

RUN cd /usr/lib/hadoop-2.7.1.2.3.4.0-3485 && \
    rm -rf share/doc share/hadoop/kms share/hadoop/httpfs; \
    find . -name '*sources*jar' -exec rm -f {} \;

RUN addgroup hadoop; \
    for n in hdfs mapred yarn zookeeper; do \
      echo -e "hadoop\nhadoop" adduser -S -G hadoop $n; \
    done
    
RUN getent passwd hdfs

RUN mkdir -p /data1/hdfs /data1/mapred /data1/yarn /var/log/hadoop /var/log/hadoop-yarn /var/log/supervisor /var/log/consul /var/lib/consul/data /var/lib/consul/ui /etc/consul /etc/consul-leader /var/lib/zookeeper; \
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
COPY consul/consul.conf /etc/supervisor/conf.d/
COPY consul /etc/consul/
COPY consul/consul.json /etc/consul-leader/

USER hdfs
RUN HADOOP_ROOT_LOGGER="WARN,console" /usr/bin/hdfs namenode -format
USER root
VOLUME /etc/hadoop/conf
