FROM centos:centos7
MAINTAINER Steffen Roegner 'steffen.roegner@gmail.com'

RUN rpm -ivh http://epel.mirror.constant.com/7/x86_64/e/epel-release-7-5.noarch.rpm; \
    yum -y install java-1.8.0-openjdk-devel
RUN yum -y install https://download.elastic.co/elasticsearch/elasticsearch/elasticsearch-1.5.2.noarch.rpm; \
    /usr/share/elasticsearch/bin/plugin -install mobz/elasticsearch-head; \
    /usr/share/elasticsearch/bin/plugin -install elasticsearch/marvel/latest; \
    usermod -s /usr/bin/bash elasticsearch

USER elasticsearch
ENV CONF_DIR /etc/elasticsearch
ENV CONF_FILE /etc/elasticsearch/elasticsearch.yml
ENV ES_HOME /usr/share/elasticsearch
ENV LOG_DIR /var/log/elasticsearch
ENV DATA_DIR /var/lib/elasticsearch
ENV WORK_DIR /tmp/elasticsearch

CMD ["/usr/share/elasticsearch/bin/elasticsearch", "-p", "/var/run/elasticsearch/elasticsearch.pid", \
                            "-Des.default.config=$CONF_FILE", \
                            "-Des.default.path.home=$ES_HOME", \
                            "-Des.default.path.logs=$LOG_DIR", \
                            "-Des.default.path.data=$DATA_DIR", \
                            "-Des.default.path.work=$WORK_DIR", \
                            "-Des.default.path.conf=$CONF_DIR"]
