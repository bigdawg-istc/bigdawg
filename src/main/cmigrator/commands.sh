./postgres2scidb -i data/fromPostgresIntDoubleString.bin -o data/toSciDBIntDoubleString.bin -f'int32_t,int32_t null,double,double null,string,string null'

./data-migrator postgres2scidb -i /home/adam/data/scidb/bool_table_test.bin -o /home/adam/data/scidb/bool_table_test_to_scidb.bin -f'bool null'

copy binary data to vertica:
./data-migrator-exe -t scidb2vertica -i /home/${USER}/data/names_scidb.bin -o /home/${USER}/data/names_vertica.bin -f string
vsql: copy names from '/home/adam/data/names_vertica.bin' native direct;
