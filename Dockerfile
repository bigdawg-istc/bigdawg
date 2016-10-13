# Dockerfile for running the bigdawg middleware maven project
# https://hub.docker.com/_/maven/

FROM maven:3.3.9-jdk-8

# Set working directory for subsequent commands. Create directory if it doesn't exist
WORKDIR /bigdawg

# Copy the repository into the container
COPY . /bigdawg

# Clean compile
RUN mvn clean compile -P mit

# Move resources
RUN mvn resources:resources -P dev

CMD ls; mvn exec:java
