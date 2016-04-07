# CMIGRATOR docs #

We use google tests: https://github.com/google/googletest/blob/master/googletest/docs/Primer.md

## BOOST

version >= 1.60.0

You can check the boost version on Ubuntu in the following way:

$ dpkg -s libboost-dev | grep 'Version'

Version: 1.54.0.1ubuntu1

if there is no boost you can try to install it from the ubuntu repository:

sudo apt-get install libboost-all-dev

Check the version!

The version 1.54.0 is too old. We have to install the latest one.

wget https://sourceforge.net/projects/boost/files/boost/1.60.0/boost_1_60_0.tar.gz/download

tar xzvf boost_1_60_0.tar.gz

cd boost_1_60_0/

sudo apt-get update

sudo apt-get install build-essential g++ python-dev autotools-dev libicu-dev build-essential libbz2-dev libboost-all-dev

./bootstrap.sh --prefix=/usr/local

./b2

sudo ./b2 install

you can add the following entries to: .bashrc

export LIBS="-L/usr/local/lib":$LIBS

export CPPFLAGS="-I/usr/local/include/boost"

export BOOST_INCLUDEDIR=/usr/local/include/boost/

export BOOST_LIBRARYDIR=/usr/local/lib/

export BOOST_ROOT=/home/adam/Downloads/boost_1_60_0

And then in your makefiles you can add: CPPFLAGS += -L/usr/local/include/boost

