#!/bin/bash

# Build the catalog database
cd /bdsetup
psql -f catalog_inserts.sstore.sql -d bigdawg_catalog
