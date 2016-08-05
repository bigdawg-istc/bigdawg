# build the project
ant clean  build -Dbuild=debug -Dsite.exec_ee_log_level=INFO
#modfiy properties/benchmarks/ycsb.properties:
#-fixed_size = false
#-num_records = 100000
#+fixed_size = true
#+num_records = 100

ant hstore-prepare -Dproject=ycsb -Dhosts="localhost:0:0"
ant hstore-benchmark -Dproject=ycsb  -Dnoexecute=true -Dnoshutdown=true
# in tab 2
ant hstore-invoke -Dproject=ycsb -Dproc=@ExtractionRemote -Dparam0=0 -Dparam1=usertable -Dparam2=psql

# You can see in: logs/sites/site-00-localhost.log 
# that in src/ee/execution/VoltDBEngine.cpp:1321 the ExtractionRemote sysproc scans a table. Here we can add a function that takes a tuple or set of tuples to do the transformation. Right now there is a string passed down to the EE to specify a destination shim. We can expand this some if we need.
