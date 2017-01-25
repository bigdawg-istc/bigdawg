# centos:centos6
# adding some packages and a jdk all needed in hadoop
# + openssh server run by supervisor

#FROM sroegner/centos-base:6
FROM centos:centos7
MAINTAINER Steffen Roegner "steffen.roegner@gmail.com"
USER root

ENV REFRESHED_AT 2015-01-31
ENV JAVA_HOME /usr
ENV JAVA /usr

RUN rpm -ivh http://epel.mirror.constant.com/7/x86_64/e/epel-release-7-5.noarch.rpm

RUN yum -y -q upgrade; \
    yum -y install passwd python-pip openssl snappy lzo sudo openssh-server openssh-clients rsync which tar bind-utils java-1.7.0-openjdk-devel; \
    yum clean all; \
    pip install supervisor 

RUN mkdir -p /root/.ssh; \
    chmod 700 /root/.ssh; \
    mkdir -p /var/run/sshd; \
    chmod 700 /var/run/sshd; \
    sed -i "s/GSSAPIAuthentication yes/GSSAPIAuthentication no/" /etc/ssh/sshd_config; \
    /usr/sbin/sshd-keygen; \
    ssh-keygen -q -t dsa -f /root/.ssh/id_dsa -N '' -C 'keypair generated during docker build' && cat /root/.ssh/id_dsa.pub > /root/.ssh/authorized_keys; \
    chmod 600 /root/.ssh/authorized_keys; \
    echo changeme | passwd --stdin root; \
    mkdir -p /etc/supervisor/conf.d /var/log/supervisor

COPY supervisord-sshd.conf /etc/supervisor/conf.d/sshd.conf
COPY supervisord.conf /etc/
COPY insecure.pub /tmp/

RUN cat /tmp/insecure.pub >> /root/.ssh/authorized_keys

EXPOSE 22

CMD ["/usr/bin/supervisord", "-n"]
