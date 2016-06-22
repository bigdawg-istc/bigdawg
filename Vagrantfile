# -*- mode: ruby -*-
# vi: set ft=ruby :

# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.vm.box = "ubuntu/trusty64"

  # port forwarding so that the VM is accessible the local machine
  config.vm.network "forwarded_port", guest: 8080, host: 8080

  # postgres1 & postgres 2
  config.vm.network "forwarded_port", guest: 5431, host: 5431
  config.vm.network "forwarded_port", guest: 5430, host: 5430

  config.vm.hostname = "bigdawg-local.mit.edu"
  config.vm.provision "shell", path: "provisions/vagrant.sh"

  config.vm.provider "virtualbox" do |vm|
    vm.name = "bigdawg-local"
    vm.memory = 2048
    #vm.cpus = 2
    # Set the timesync threshold to 1 minute, instead of the default 20 minutes.
    vm.customize ["guestproperty", "set", :id, "/VirtualBox/GuestAdd/VBoxService/--timesync-set-threshold", 60000]
  end
end