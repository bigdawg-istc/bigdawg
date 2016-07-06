FROM maven:3.3.9-jdk-8

COPY . /usr/src/app

# doesn't yet navigate to the directory where this needs to be run

COPY . /usr/src/app

RUN cd /usr/src/app; mvn install -P dev