#!/bin/bash

pguser=pguser

# Create user
#   -s: Superuser
#   -d: User will be able to create databases
#   -e: Echo the commands
gosu postgres createuser -s -d -e ${pguser}
gosu postgres psql -c "alter role ${pguser} with password 'test'"


# mimic2_copy
gosu postgres psql -c "create database mimic2_copy owner ${pguser}"
gosu postgres psql -f /tmp/mimic2/mimic2.pgd -U ${pguser} -d mimic2_copy  # tables are created inside the mimic2.pgd script


# test database
gosu postgres psql -c "create database test owner ${pguser}"


echo "*****\n==> Done with database setup.\n*****"docker run --rm -it --net=bigdawg --name bigdawg-postgres2 bigdawg/postgres2
