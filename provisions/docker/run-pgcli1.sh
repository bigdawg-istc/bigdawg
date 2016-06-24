#!/bin/bash

if (( EUID != 0 )); then
   echo "Script must be run as root."
   exit 126
fi

if `docker inspect -f {{.State.Running}} postgres1`; then
    echo "Starting pgcli container for postgres1..."
    (set -x; docker run -ti --rm \
        --net=bigdawg \
        diyan/pgcli --host postgres1 --user postgres -w)
    echo "Closing pgcli postgres1 container."
else
    echo "postgres1 container not running. Skipping pgcli1."
fi