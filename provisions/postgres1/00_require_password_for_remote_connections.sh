#!/bin/bash

# Require a password for remote connections
gosu postgres sed -i -e 's/host all all 0.0.0.0\/0 trust/host all all 0.0.0.0\/0 md5/g' \
    /var/lib/postgresql/data/pg_hba.conf
