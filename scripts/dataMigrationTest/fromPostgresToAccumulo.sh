javaArgs="-XX:-UseConcMarkSweepGC -Xmx3g -Xms1g"
dbname=tpch
ARRAY_TPCH=(region nation supplier customer part partsupp orders lineitem)
ARRAY=${ARRAY_TPCH[*]}
dbgen_dir=/home/adam/Chicago/tpch-generator/dbgen/
dbgen="dbgen"

currentDir=$(pwd)
#for size in 0.07 0.08 0.09 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9 1.0 1.1 1.2 1.3 1.4 1.5 1.6 1.7 1.8 1.9 2.0 3.0 4.0 5.0; do
for size in 0.02; do
    cd ${dbgen_dir}
    rm *.csv
    rm *.tbl
    ./dbgen -vf -s ${size}
    ./rename_from_tbl_to_csv.sh 
    cd /home/adam/data-loading/scripts/
    bash run_benchmarks.sh ${size} ${dbgen}
    cd /home/adam/bigdawgmiddle
    mvn exec:java -Dexec.mainClass=istc.bigdawg.migration.TpchTpchPostgresAccumulo -Dexec.args="all ${size} ${javaArgs}"
    #mvn exec:java -Dexec.mainClass=istc.bigdawg.accumulo.TpchFromPostgresToAccumulo -Dexec.args="countRowsAccumulo ${size} ${javaArgs}"
    cd ${currentDir}
    for table in ${ARRAY[*]}; do
	select="select count(*) from ${table}"
	echo $select
	tuples=$(echo $select | psql -d $dbname -P t -P format=unaligned)
	echo $table, $tuples
	all_tuples=$((all_tuples+tuples))
    done
    echo TIMESTAMP=$(date -d"$CURRENT +$MINUTES minutes" '+%F_%T.%N_%Z'):all_tuples: $all_tuples:size:$size >> fromPostgresToAccumulo.log
done
