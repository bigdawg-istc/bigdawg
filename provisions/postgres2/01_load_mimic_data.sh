#!/bin/bash

database2=mimic2_copy

# main user
pguser=pguser


gosu postgres psql -c "alter role postgres with password 'test'" -d template1

# clean the databases

gosu postgres createuser -s -e -d ${pguser}
gosu postgres psql -c "alter role ${pguser} with password 'test'" -d template1

gosu postgres psql -c "create database ${database2} owner ${pguser}" -d template1

# May return an error about there not being a root role. It's not an issue, so ignore
gosu postgres psql -f /tmp/mimic2.pgd -U ${pguser} -d ${database2}

# tests
gosu postgres psql -c "create database test owner ${pguser}" -d template1 -U postgres

echo "All done."
