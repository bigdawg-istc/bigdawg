FROM sroegner/accumulo

# Install octave. Needed for data loading.
RUN yum install -y epel-release && \
	yum install -y octave

COPY image_contents /image_contents/

CMD /image_contents/start_services.sh