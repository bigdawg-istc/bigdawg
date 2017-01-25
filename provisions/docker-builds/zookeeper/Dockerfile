FROM sroegner/centos-base-ssh:7
MAINTAINER Steffen Roegner 'steffen.roegner@gmail.com'

ENV ZOO_LOG4J_PROP WARN,CONSOLE

RUN curl -L http://public-repo-1.hortonworks.com/HDP/centos6/2.x/GA/2.2.0.0/hdp.repo -o /etc/yum.repos.d/hdp.repo; \
    yum -y install zookeeper-server; \
    mkdir -p /var/lib/zookeeper; chown zookeeper:hadoop /var/lib/zookeeper

COPY conf.supervisor/zookeeper.conf /etc/supervisor/conf.d/
COPY conf.zk/zoo.cfg /etc/zookeeper/conf/
RUN echo "export ZOO_LOG4J_PROP=WARN,CONSOLE" > /etc/zookeeper/conf/zooekeeper-env.sh

EXPOSE 2181

USER zookeeper
ENTRYPOINT ["/usr/hdp/current/zookeeper-client/bin/zkServer.sh", "start-foreground"]

