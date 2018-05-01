FROM ubuntu:14.04

# Install java and other utils
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y software-properties-common && \
    add-apt-repository ppa:webupd8team/java -y && \
    apt-get update && \
    echo oracle-java7-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections && \
    apt-get install -y oracle-java8-installer && \
    apt-get install -y curl && \
    apt-get install -y netcat && \
    apt-get install -y iputils-ping && \
    apt-get install -y net-tools && \
    apt-get clean

# modify permissions
RUN apt-get update && \
  DEBIAN_FRONTEND=noninteractive apt-get install -y mysql-server && \
  echo '[mysqld]\n bind-address = 0.0.0.0\n\n[client]\nsocket=/var/run/mysql/mysql.sock\nhost=0.0.0.0\nport=3306' > /etc/mysql/my.cnf && \
  chmod -R 0700 /etc/ssl/private && \
  chown -R mysql /etc/ssl/private && \
  mkdir /home/mysql && \
  chown mysql /home/mysql

# Copy over the mimic2 sql script into the filesystem
COPY mimic2_mysql.sql /home/mysql
COPY start_services.sh /home/mysql
RUN chmod +x /home/mysql/start_services.sh

# Configure mysql with user "mysqluser" and password "test"
USER mysql
RUN mysqld & \
    while !(mysqladmin ping); do sleep 3; done && \
    mysql --user=root -v -e "GRANT ALL PRIVILEGES ON *.* TO 'mysqluser'@'%' IDENTIFIED BY PASSWORD '*94BDCEBE19083CE2A1F959FD02F964C7AF4CFC29' WITH GRANT OPTION;" && \
    /etc/init.d/mysql stop

CMD /home/mysql/start_services.sh