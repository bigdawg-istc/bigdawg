FROM maven:3.3.9-jdk-8

VOLUME /bigdawg

CMD cd /bigdawg; mvn exec:java -P dev
