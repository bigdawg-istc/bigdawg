FROM sroegner/centos-base-ssh:6
MAINTAINER Steffen Roegner 'steffen.roegner@gmail.com'

RUN yum -y install createrepo; \
    mkdir /tmp/nexus && \
    cd /tmp/nexus && \
    curl -L http://download.sonatype.com/nexus/oss/nexus-latest-bundle.tar.gz | tar xz --no-same-owner

RUN useradd -m -d /srv/nexus nexus

RUN mv -v $(ls -1d /tmp/nexus/nex*|head -1) /srv/nexus/; \
    ln -s $(ls -1d /srv/nexus/nexus-*|head -1) /srv/nexus/nexus-server; \
    mv -v /tmp/nexus/sonatype-work /srv/nexus && \
    chown -R nexus:nexus /srv/nexus/*
    
COPY nexus.properties /srv/nexus/nexus-server/conf/nexus.properties

EXPOSE 8081
USER nexus
WORKDIR /srv/nexus/nexus-server

ENTRYPOINT ["/srv/nexus/nexus-server/bin/jsw/linux-x86-64/wrapper", "-c", "/srv/nexus/nexus-server/bin/jsw/conf/wrapper.conf"]

