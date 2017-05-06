#!/bin/bash

echo
echo "========================================"
echo "===== Setting Catalog for S-Store ======"
echo "========================================"

# sstore
docker exec bigdawg-postgres-catalog /bdsetup/setup_sstore.sh

echo
echo "================="
echo "===== Done. ====="
echo "================="
