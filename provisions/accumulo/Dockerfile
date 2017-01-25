FROM bigdawg/accumulo-base

# Install octave. Needed for data loading.
RUN yum install -y epel-release && \
	yum install -y octave

CMD ["/usr/bin/supervisord", "-n"]