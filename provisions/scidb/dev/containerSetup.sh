#!/bin/bash
export LC_ALL="en_US.UTF-8"
chmod u+s /usr/bin/sudo

echo "--------------------------------------------------"
echo "Installing additional software (as root)..."
echo "--------------------------------------------------"
./installR.sh
Rscript /home/scidb/installPackages.R packages=scidb verbose=0 quiet=0

echo "--------------------------------------------------"
echo "Starting SSH (as root)..."
echo "--------------------------------------------------"
/usr/sbin/update-rc.d ssh defaults
sudo service ssh start # coordinator only

echo "--------------------------------------------------"
echo "Creating root's SSH certificate (as root)..."
echo "--------------------------------------------------"
yes | ssh-keygen -f ~/.ssh/id_rsa -t rsa -N ''
chmod 755 ~
chmod 755 ~/.ssh




echo "--------------------------------------------------"
echo "Switching to user scidb (as root)..."
#sudo su scidb
echo "--------------------------------------------------"
su scidb <<'EOF'

cd ~
export LC_ALL="en_US.UTF-8"

echo "--------------------------------------------------"
echo "Environmental variables to bashrc..."
echo "--------------------------------------------------"
export SCIDB_VER=15.12
# single node installation
export SCIDB_INSTALL_PATH=/home/scidb/dev_dir/scidbtrunk/stage/install
# NO single node installation
#export SCIDB_INSTALL_PATH=/opt/scidb/15.12
export SCIDB_BUILD_TYPE=RelWithDebInfo # Debug
export PATH=$SCIDB_INSTALL_PATH/bin:$PATH
# write to bashrc
echo "################## SCIDB ############################" >> ~/.bashrc
echo "export SCIDB_VER=$SCIDB_VER" >> ~/.bashrc
echo "export SCIDB_INSTALL_PATH=$SCIDB_INSTALL_PATH" >> ~/.bashrc
echo "export SCIDB_BUILD_TYPE=$SCIDB_BUILD_TYPE" >> ~/.bashrc
echo "export PATH=$SCIDB_INSTALL_PATH:$PATH" >> ~/.bashrc
#source ~/.bashrc


echo "--------------------------------------------------"
echo "Renaming source-code folder..."
echo "--------------------------------------------------"
mv /home/scidb/dev_dir/scidb-15.12.1.4cadab5 /home/scidb/dev_dir/scidbtrunk


echo "--------------------------------------------------"
echo "Creating scidb's SSH certificate..."
echo "--------------------------------------------------"
yes | ssh-keygen -f ~/.ssh/id_rsa -t rsa -N ''
chmod 755 ~
chmod 755 ~/.ssh
exec ssh-agent bash
ssh-add


echo "--------------------------------------------------"
echo "Providing Passwordless SSH..."
# omit server's IP for single host deployment
echo "--------------------------------------------------"
echo xxxx.xxxx.xxxx | /home/scidb/dev_dir/scidbtrunk/deployment/deploy.sh access root "" "" localhost
echo xxxx.xxxx.xxxx | /home/scidb/dev_dir/scidbtrunk/deployment/deploy.sh access scidb "" "" localhost


echo "--------------------------------------------------"
echo "Installing build tools..."
echo "--------------------------------------------------"
/home/scidb/dev_dir/scidbtrunk/deployment/./deploy.sh prepare_toolchain localhost
/home/scidb/dev_dir/scidbtrunk/deployment/./deploy.sh prepare_coordinator localhost # Not all the machines are coordinators

echo "--------------------------------------------------"
echo "Preparing a chroot..."
echo "--------------------------------------------------"
echo xxxx.xxxx.xxxx | /home/scidb/dev_dir/scidbtrunk/deployment/deploy.sh prepare_chroot scidb localhost

echo "--------------------------------------------------"
echo "Setting PostGRESQL up (as scidb)..."
echo "--------------------------------------------------"
/home/scidb/dev_dir/scidbtrunk/deployment/deploy.sh prepare_postgresql postgres postgres 255.255.0.0/16 localhost


echo "--------------------------------------------------"
echo "Leaving SciDB user..."
echo "--------------------------------------------------"
#exit
EOF




echo "--------------------------------------------------"
echo "Setting PostGRESQL up (as root)..."
echo "--------------------------------------------------"
usermod -G scidb -a postgres
chmod g+rx /home/scidb/dev_dir
sudo -u postgres ls /home/scidb/dev_dir/ # test




echo "--------------------------------------------------"
echo "Switching back to user scidb (as root)..."
#sudo su scidb
echo "--------------------------------------------------"
su scidb <<'EOF'
cd ~
export LC_ALL="en_US.UTF-8"


echo "--------------------------------------------------"
echo "Building SciDB..."
echo "--------------------------------------------------"
yes | /home/scidb/dev_dir/scidbtrunk/./run.py setup
n=`cat /proc/cpuinfo | grep "cpu cores" | uniq | awk '{print $NF}'` # get the number of cores
/home/scidb/dev_dir/scidbtrunk/./run.py make -j $n


echo "--------------------------------------------------"
echo "Installing SciDB..."
# If you provide the environmental variable SCIDB_INSTALL_PATH, SciDB is installed there.
# Otherwise, SciDB installs to ./stage/install.
echo "--------------------------------------------------"
yes | /home/scidb/dev_dir/scidbtrunk/./run.py install


echo "--------------------------------------------------"
echo "Building packages for a multi-node installation..."
# This produces the packages to install in a multi-node environment.
echo "--------------------------------------------------"
/home/scidb/dev_dir/scidbtrunk/deployment/./deploy.sh build RelWithDebInfo /tmp/packages


echo "--------------------------------------------------"
echo "Moving multi-node packages..."
echo "--------------------------------------------------"
mkdir /tmp/dbg_packages
mv /tmp/packages/*dbg* /tmp/dbg_packages


echo "--------------------------------------------------"
echo "Install multinode packages..."
echo "--------------------------------------------------"
/home/scidb/dev_dir/scidbtrunk/deployment/./deploy.sh scidb_install /tmp/packages localhost


echo "--------------------------------------------------"
echo "Placing configuration file (as root)..."
echo "--------------------------------------------------"
echo xxxx.xxxx.xxxx | sudo cp /home/scidb/scidb_docker.ini $SCIDB_INSTALL_PATH/etc/config.ini


echo "--------------------------------------------------"
echo "Leaving SciDB user..."
echo "--------------------------------------------------"
#exit
EOF




echo "--------------------------------------------------"
echo "Starting PostGRESQL (as root)..."
echo "--------------------------------------------------"
/etc/init.d/postgresql start







#--------------
#sudo su scidb
su scidb <<'EOF'
cd ~
export LC_ALL="en_US.UTF-8"
echo xxxx.xxxx.xxxx | sudo /home/scidb/dev_dir/scidbtrunk/deployment/./deploy.sh scidb_prepare scidb xxxx.xxxx.xxxx scidb xxxx.xxxx.xxxx scidb_docker /home/scidb/data 7 default 1 default localhost localhost
#                                                         deployment/./deploy.sh scidb_prepare scidb "" mydb mydb mydb /home/scidb/mydb-DB 2 default 1 default <hostIP0> <hostIP1>










scidb.py startall scidb_docker

#------------ local development ---------------------
#------------ cluster development -------------------
#echo xxxx.xxxx.xxxx | /home/scidb/dev_dir/scidbtrunk/deployment/./deploy.sh scidb_start scidb scidb_docker localhost
#----------------------------------------------------
iquery -naq "store(build(<num:double>[x=0:4,1,0, y=0:6,1,0], random()),TEST_ARRAY)"
iquery -aq "list('arrays')"
iquery -aq "scan(TEST_ARRAY)"
scidb.py stopall scidb_docker
#exit
EOF
#--------------
