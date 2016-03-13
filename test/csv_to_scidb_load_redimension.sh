csv_file=$1
size=$2
scidb_array=$3
scidb_pipe=$4

START=$(date +%s.%N)
time csv2scidb -i ${csv_file} -o ${scidb_pipe} -p NNN -s 1 -d ',' &
time iquery -anq "store(redimension(load(${scidb_array},'${scidb_pipe}',-2,'text',0),target),target)" &
wait
END=$(date +%s.%N)
FROM_CSV_TO_SCIDB=$(echo "$END - $START" | bc)
TIMESTAMP=$(date -d"$CURRENT +$MINUTES minutes" '+%F_%T.%N_%Z')
scidb_count=$(iquery -q "select count(*) from ${scidb_array}" | sed -n 2p | cut -d " " -f2)
scidb_count2=$(iquery -q "select count(*) from target" | sed -n 2p | cut -d " " -f2)
echo $TIMESTAMP, size: $size, from csv to scidb format and load scidb: ${FROM_CSV_TO_SCIDB}, scidb count elements, ${scidb_count}, target count, ${scidb_count2} >> from_csv_to_scidb_load_redimension_full.log


