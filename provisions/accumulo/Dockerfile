FROM bigdawg/accumulo-base

# Install octave. Needed for data loading.
RUN yum install -y epel-release && \
	yum install -y octave

COPY d4m_api /

CMD ["/usr/bin/supervisord", "-n"]