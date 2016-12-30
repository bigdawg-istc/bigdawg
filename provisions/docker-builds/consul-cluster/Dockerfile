FROM centos:7
MAINTAINER Steffen Roegner 'steffen.roegner@gmail.com'

ENV CONSUL_VERSION 0.6.3

RUN yum -y install curl unzip
RUN mkdir -p /var/log/consul /etc/consul /var/lib/consul/data /var/lib/consul/ui /var/lib/consul/scripts
RUN curl --fail -q -L https://releases.hashicorp.com/consul/${CONSUL_VERSION}/consul_${CONSUL_VERSION}_linux_amd64.zip -o /tmp/c.zip && \
    curl --fail -q -L https://releases.hashicorp.com/consul/${CONSUL_VERSION}/consul_${CONSUL_VERSION}_web_ui.zip -o /tmp/ui.zip && \
    unzip /tmp/c.zip -d /usr/sbin && \
    unzip /tmp/ui.zip -d /var/lib/consul/ui

COPY *.json /etc/consul/ 
COPY checkscripts /var/lib/consul/scripts/

EXPOSE 8400 8500 8600

CMD ["/usr/bin/supervisord", "-n"]
