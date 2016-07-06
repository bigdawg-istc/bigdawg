FROM maven:3.3.9-jdk-8

COPY . /usr/src/app

RUN cd /usr/src/app; mvn install -P dev -D skipTests