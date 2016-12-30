FROM ubuntu:14.04
MAINTAINER Steffen Roegner "steffen.roegner@gmail.com"
USER root

ENV REFRESHED_AT 2015-08-14
 
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update
RUN locale-gen en_US en_US.UTF-8
ENV LANG en_US.UTF-8
RUN echo "export PS1='\e[1;31m\]\u@\h:\w\\$\[\e[0m\] '" >> /root/.bashrc

RUN apt-get install -y curl default-jre libwebkitgtk-1.0-0 lame xterm

RUN curl --fail --silent -L http://www.xmind.net/xmind/downloads/xmind-7-update1-linux_amd64.deb -o /tmp/xmind.deb && \
    dpkg -i /tmp/xmind.deb

ENV DISPLAY :0
CMD ["/usr/bin/XMind"]
