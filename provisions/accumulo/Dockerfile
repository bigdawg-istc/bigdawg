FROM bigdawg/accumulo-base

# Install octave. Needed for data loading.
RUN yum install -y epel-release && \
	yum install -y octave

# Copy the middleware jar (build script should move this here)
COPY istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar /

CMD ["/usr/bin/supervisord", "-n"]