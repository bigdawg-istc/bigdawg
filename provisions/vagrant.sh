# Add Docker apt repo gpg key
apt-key adv --keyserver hkp://pgp.mit.edu:80 --recv-keys \
    58118E89F3A912897C070ADBF76221572C52609D
echo "deb https://apt.dockerproject.org/repo ubuntu-trusty main" > \
    /etc/apt/sources.list.d/docker.list
apt-get update
apt-get install -y docker-engine

# # Make scripts available in shell as bd-* commands
for f in /vagrant/provisions/docker/*.sh; do
    ln -s $f /usr/local/sbin/bd-$(basename $f .sh);
done
source ~/.profile

bd-download-test-data
# bd-build-images
# bd-create-dev-containers
