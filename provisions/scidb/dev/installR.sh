#!/bin/bash
export LC_ALL="en_US.UTF-8"
echo "##################################################"
echo "INSTALL R IN UBUNTU 12"
echo "##################################################"
sudo echo "# R MIRROR" >> /etc/apt/sources.list
sudo echo "deb http://cran.r-project.org/bin/linux/ubuntu precise/" >> /etc/apt/sources.list
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E084DAB9
sudo apt-get update
yes | sudo apt-get install r-base r-base-dev


