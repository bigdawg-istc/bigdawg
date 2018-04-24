FROM centos:centos6
MAINTAINER Joe Arasin <joe.arasin@bluelabs.com>

# Update the image
RUN yum update -y; yum clean all

# Install Dependencies
RUN yum install -y openssl which mcelog gdb sysstat sudo
RUN yum install -y openssh-server openssh-clients

# grab gosu for easy step-down from root
RUN yum install -y curl \
	&& curl -o /usr/local/bin/gosu -SL 'https://github.com/tianon/gosu/releases/download/1.1/gosu' \
	&& chmod +x /usr/local/bin/gosu

RUN yum clean all

ENV LANG en_US.utf8
ENV TZ "US/Eastern"

RUN groupadd -r verticadba
RUN useradd -r -m -g verticadba dbadmin

ADD vertica-*.rpm /rpms/vertica.rpm

RUN yum install -y /rpms/vertica.rpm

# In theory, someone should make things work without ignoring the errors.
# But that's in theory, and for now, this seems sufficient.
RUN /opt/vertica/sbin/install_vertica --license CE --accept-eula --hosts 127.0.0.1 --dba-user-password-disabled --failure-threshold NONE --no-system-configuration

USER dbadmin
RUN /opt/vertica/bin/admintools -t create_db -s localhost --skip-fs-checks -d docker -c /home/dbadmin/docker/catalog -D /home/dbadmin/docker/data
USER root

RUN mkdir /tmp/.python-eggs
RUN chown -R dbadmin /tmp/.python-eggs
ENV PYTHON_EGG_CACHE /tmp/.python-eggs

ENV VERTICADATA /home/dbadmin/docker
VOLUME  /home/dbadmin/docker

# Add mimic insert script
COPY mimic_dump_vertica.sql /home/dbadmin

ADD ./docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]

EXPOSE 5433
