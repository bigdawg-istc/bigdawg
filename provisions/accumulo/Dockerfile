FROM bigdawg/accumulo-base

# Install octave. Needed for data loading.
RUN yum install -y epel-release && \
	yum install -y octave

# Copy the middleware jar (build script should move this here)
COPY PostgresParserTerms.csv /src/main/resources/PostgresParserTerms.csv
COPY SciDBParserTerms.csv /src/main/resources/SciDBParserTerms.csv
COPY istc.bigdawg-1.0-SNAPSHOT-jar-with-dependencies.jar /

CMD ["/usr/bin/supervisord", "-n"]