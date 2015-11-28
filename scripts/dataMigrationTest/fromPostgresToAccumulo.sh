javaArgs="-XX:-UseConcMarkSweepGC -Xmx3g -Xms1g"
dbname=tpch
ARRAY_TPCH=(region nation supplier customer part partsupp orders lineitem)
ARRAY=${ARRAY_TPCH[*]}

currentDir=$(pwd)
for size in 0.05 0.1 0.5 1; do
    cd /home/adam/data-loading/scripts/
    bash run_benchmarks.sh ${size}
    cd /home/adam/bigdawgmiddle
    mvn exec:java -Dexec.mainClass=istc.bigdawg.accumulo.TpchFromPostgresToAccumulo -Dexec.args="fromPostgresToAccumulo ${size} ${javaArgs}"
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