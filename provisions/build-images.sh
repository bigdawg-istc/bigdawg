if (( EUID != 0 )); then
   echo "Script must be run as root."
   exit 126
fi


# Default use case is to run BigDawg in a Vagrant VM. Nonstandard setups should run this script from the BigDawg working directory.
cd /vagrant &> /dev/null

# Build the project's Docker images if you don't want to pull the prebuilt ones from Docker Hub
echo "Building Docker images..."
echo "Building postgres1 (m/n)"
docker build -t postgres1 provisions/postgres1/

echo "Building postgres2 (m/n)"
docker build -t postgres1 provisions/postgres2/