#!/bin/sh

RESULTS=$1
DBNAME=$2
USER=$3

# delay between stats collections (iostat, vm_stat, ...)
DELAY=15

# DSS queries timeout (5 minutes or something like that)
DSS_TIMEOUT=300 # 5 minutes in seconds

# log
LOGFILE=bench.log

function benchmark_run() {

	mkdir -p $RESULTS

	print_log "  analyzing"

	psql -h localhost -U $USER $DBNAME -c "analyze" > $RESULTS/analyze.log 2> $RESULTS/analyze.err

	print_log "running TPC-H benchmark"

  psql -h localhost -U $USER $DBNAME < working.sql > results.log 2> results.err
    
	print_log "finished TPC-H benchmark"

}



function print_log() {

	local message=$1

	echo `date +"%Y-%m-%d %H:%M:%S"` "["`date +%s`"] : $message" >> $RESULTS/$LOGFILE;

}

mkdir $RESULTS;

# run the benchmark
benchmark_run $RESULTS $DBNAME $USER

