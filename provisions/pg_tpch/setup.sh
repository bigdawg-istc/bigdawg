#!/bin/bash
set -ex

DBNAME='tpch'
USER='pguser'
PASSWORD='test'

# Insert data into database
echo "  loading data"

psql -h localhost -U $USER $DBNAME < dss/tpch-load.sql

echo "  creating primary keys"

psql -h localhost -U $USER $DBNAME < dss/tpch-pkeys.sql

echo "  creating foreign keys"

psql -h localhost -U $USER $DBNAME < dss/tpch-alter.sql

echo "  creating indexes"

psql -h localhost -U $USER $DBNAME < dss/tpch-index.sql

