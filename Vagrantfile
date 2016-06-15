# -*- mode: ruby -*-
# vi: set ft=ruby :

# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.vm.box = "ubuntu/trusty64"

  config.vm.network "private_network", ip: "192.168.50.10"
  config.vm.hostname = "bigdawg-local.mit.edu"

  config.vm.provider "virtualbox" do |vm|
    vm.name = "bigdawg-local"
    vm.memory = 2048
    #vm.cpus = 2
    # Set the timesync threshold to 1 minute, instead of the default 20 minutes.
    vm.customize ["guestproperty", "set", :id, "/VirtualBox/GuestAdd/VBoxService/--timesync-set-threshold", 60000]
  end
end