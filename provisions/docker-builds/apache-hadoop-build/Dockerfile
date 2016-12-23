FROM centos:7
MAINTAINER Steffen Roegner "steffen.roegner@gmail.com"
USER root

ENV JAVA_HOME=/usr
ENV REFRESHED_AT 2015-09-03
ENV M2_HOME=/usr/local

RUN rpm -ivh http://epel.mirror.constant.com/7/x86_64/e/epel-release-7-5.noarch.rpm; \
    yum -y -q upgrade; \
    yum -y install python-pip snappy lzo rsync which tar bind-utils java-1.7.0-openjdk-devel unzip bzip2; \
    yum clean all

RUN yum -y install gcc automake zlib-devel openssl-devel autoreconf
RUN cd /usr/local && curl -L http://www.us.apache.org/dist/maven/maven-3/3.3.3/binaries/apache-maven-3.3.3-bin.tar.gz | tar xz --strip-components=1
RUN mkdir /protoc && cd /protoc && curl -L https://github.com/google/protobuf/archive/v2.5.0.tar.gz | tar xz --strip-components=1 && \
    ./autogen.sh; ./configure --prefix=/usr; make; make install;
    protoc --version
RUN cd /protoc/java && /usr/local/bin/mvn install


ADD build_hadoop.sh /

RUN chmod 755 /build_hadoop.sh
