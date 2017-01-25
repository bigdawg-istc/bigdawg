FROM sroegner/centos-base-ssh:6
MAINTAINER Steffen Roegner 'steffen.roegner@gmail.com'


RUN yum -y install dnsmasq

COPY dnsmasq.conf /etc/
COPY resolv.dnsmasq.conf /etc/

VOLUME /dnsmasq.hosts

EXPOSE 5353

ENTRYPOINT ["/usr/sbin/dnsmasq", "-d"]

