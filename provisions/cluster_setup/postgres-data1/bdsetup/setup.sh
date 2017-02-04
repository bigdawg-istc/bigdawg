#!/bin/bash

# Create directory expected by the import script below. Run as root.
mkdir -p /var/lib/postgresql/9.4/main/mimic2v26_dat
chown postgres /var/lib/postgresql/9.4/main/mimic2v26_dat

# Insert the first n records of the mimic dataset, create "test" database. Run as postgres.
su postgres <<'EOF'
whoami
cd /var/lib/postgresql
tar -xvf /bdsetup/mimic2_flatfiles.tar.gz --directory /var/lib/postgresql
cd MIMIC-Importer-2.6
./import.sh -s 0 -e 99
psql -c "ALTER DATABASE \"MIMIC2\" RENAME TO mimic2"
psql -c "create database test owner pguser"
EOF