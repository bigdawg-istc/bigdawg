# Dockerfile for running the bigdawg middleware maven project
# See https://hub.docker.com/_/maven/
# Build this image with docker build -t bigdawg/middleware .

FROM maven:3.3.9-jdk-8

# Set working directory for subsequent commands. Creates directory if it doesn't exist.
WORKDIR /opt/bigdawg

# Copy the project (needs pom, local maven dependencies like scidb/sstore, and profiles)
COPY . /opt/bigdawg/

# Install
ENV MAVEN_OPTS="-Xmx1500m"
RUN mvn clean install -P mit -DskipTests

# Expose local port for HTTP server
EXPOSE 8080

# Execute
#CMD ["mvn", "exec:java"]  # Inside docker, this causes a re-download of dependencies.
CMD java -jar target/istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar