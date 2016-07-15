apt-get update
apt-get install -y \
    wget \
    apt-transport-https \
    ssh \
    openssh-server \
    expect
wget -O- https://downloads.paradigm4.com/key | apt-key add --
echo "deb https://downloads.paradigm4.com/ ubuntu14.04/14.12/" >> /etc/apt/sources.list.d/scidb.list
echo "deb-src https://downloads.paradigm4.com/ ubuntu14.04/14.12/" >> /etc/apt/sources.list.d/scidb.list
apt-get update -y
