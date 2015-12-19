scale=$1
i_max=$((1*scale))
j_max=131072

chunk1=10
chunk2=100000

iquery -aq "CREATE ARRAY CFlat < i:int32, j:int32, val:double > [k=0:*,1048576,0]"
iquery -q "drop array E"
iquery -q "create array E <val:double>[i=0:${i_max},${chunk1},0,j=0:${j_max},${chunk2},0]"
/home/adam/databases/PostgreSQL/pgsql-9.4.4/bin/psql -p 5431 -d tmpfs -U ubuntu -c "copy waveform2 to STDIN with (format csv, header, freeze)" | loadcsv.py -n 1 -t NNN -a 'CFlat' -A 'E'
iquery -q "drop array CFlat"
