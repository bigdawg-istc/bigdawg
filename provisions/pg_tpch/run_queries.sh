#!/bin/bash

for i in {1..5}
do
    echo " - - - - Running iteration ${i} - - - - "
    psql -U pguser tpch < working.sql &> results/results${i}.txt
done

for i in {1..5}
do
    cat results/results${i}.txt | grep ms | awk '{print $(NF-1)}' | tr '[\n]' '[\t']
    echo "\n"
done
