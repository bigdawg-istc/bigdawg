javaArgs="-XX:-UseConcMarkSweepGC -Xmx3g -Xms1g"
dbname=tpch
ARRAY_TPCH=(region nation supplier customer part partsupp orders lineitem)
ARRAY=${ARRAY_TPCH[*]}
dbgen_dir=/home/adam/Chicago/tpch-generator/dbgen/
scripts_dir=/home/adam/data-loading/scripts/

current_dir=$(pwd)
for size in 0.05; do
    cd ${dbgen_dir}
    rm *.csv
    rm *.tbl
    ./dbgen -vf -s ${size}
    ./rename_from_tbl_to_csv.sh 
    cd ${scripts_dir}
    bash run_benchmarks.sh ${size} "dbgen"
    cd ${dbgen_dir}
    rm *.csv
    cd ${scripts_dir}
    bash run_benchmarks.sh ${size} "dbgen" "extract" # example: ./run_benchmarks.sh 0.05 dbgen extract
    cd ${current_dir}
    # extract data size
    size=$(du -s -h ${dbgen_dir} | awk '{print $1}')
    echo TIMESTAMP=$(date -d"$CURRENT +$MINUTES minutes" '+%F_%T.%N_%Z'):folder:${dbgen_dir}:size:$size >> postgresExtract.log
done