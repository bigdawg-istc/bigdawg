# Download test data into the ~/data directory

initial_dir=$(pwd)

cd ~/
mkdir data
cd data
data_dir=$(pwd)

if [ ! -f mimic2.pgd ]; then
    echo "Downloading mimic2 data..."
    wget https://bitbucket.org/adam-dziedzic/bigdawgdata/raw/6ade22253695bfeb33074e82929e83b52cb121f1/mimic2.pgd &>/dev/null
else
    echo "mimic2 data already downloaded. skipping."
fi

cd ${pwd}

echo "download test-data done"