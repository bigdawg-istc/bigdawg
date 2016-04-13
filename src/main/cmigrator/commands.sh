./postgres2scidb -i data/fromPostgresIntDoubleString.bin -o data/toSciDBIntDoubleString.bin -f'int32_t,int32_t null,double,double null,string,string null'

./data-migrator postgres2scidb -i /home/adam/data/scidb/bool_table_test.bin -o /home/adam/data/scidb/bool_table_test_to_scidb.bin -f'bool null'

