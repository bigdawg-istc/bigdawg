-- catalog.islands: iid scope_name  access_method
insert into catalog.islands values (0, 'RELATIONAL', 'PSQL');
insert into catalog.islands values (1, 'ARRAY', 'AFL');
insert into catalog.islands values (2, 'TEXT', 'JSON');

-- catalog.engines: engine_id, name, host, port, connection_properties
insert into catalog.engines values(0,'postgres0','bigdawg-postgres-catalog',5400,'PostgreSQL 9.4.5');
insert into catalog.engines values(1,'vertica','bigdawg-vertica-tpch',5433,'Vertica');
-- insert into catalog.engines values(2,'mysql','bigdawg-mysql-data',3306,'MySQL 5.7');
-- insert into catalog.engines values(3,'scidb_local','bigdawg-scidb-data',1239,'SciDB 14.12');
-- insert into catalog.engines values (4, 'saw ZooKeeper', 'zookeeper.docker.local', 2181, 'Accumulo 1.6');

-- catalog.databases: dbid, engine_id, name, userid, password
insert into catalog.databases values(0,0,'bigdawg_catalog','pguser','test');
insert into catalog.databases values(1,0,'bigdawg_schemas','pguser','test');
insert into catalog.databases values(2,1,'docker','dbadmin','test');
-- insert into catalog.databases values(3,2, 'mimic2v26', 'mysqluser', 'test');
-- insert into catalog.databases values(3,2,'mimic2_copy','pguser','test');
-- insert into catalog.databases values(4,0,'tpch','pguser','test');
-- insert into catalog.databases values(5,1,'tpch','pguser','test');
-- insert into catalog.databases values(6,3,'scidb_local','scidb','scidb123');
-- insert into catalog.databases values (7, 4, 'accumulo', 'bigdawg', 'bigdawg');

-- catalog.shims: shim_id island_id engine_id access_method
insert into catalog.shims values (0, 0, 0, 'N/A');
insert into catalog.shims values (1, 0, 1, 'N/A');
-- insert into catalog.shims values (2, 0, 2, 'N/A');
-- insert into catalog.shims values (3, 1, 3, 'N/A');
-- insert into catalog.shims values (4, 2, 4, 'N/A');

-- catalog.scidbbinapath:
-- binary path to scidb utilities: csv2scidb, iquery, etc.
insert into catalog.scidbbinpaths values (3,'/opt/scidb/14.12/bin/');


-- catalog.objects
-- oid  name  fields  logical_db  physical_db
insert into catalog.objects values(0, 'customer', 'c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,c_mktsegment,c_comment', 2, 2);
insert into catalog.objects values(1, 'lineitem', 'l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,l_extendedprice,l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment', 2, 2);
insert into catalog.objects values(2, 'nation', 'n_nationkey,n_name,n_regionkey,n_comment', 2, 2);
insert into catalog.objects values(3, 'orders', 'o_orderkey,o_custkey,o_orderstatus,o_totalprice,o_orderdate,o_orderpriority,o_clerk,o_shippriority,o_comment', 2, 2);
insert into catalog.objects values(4, 'part', 'p_partkey,p_name,p_mfgr,p_brand,p_type,p_size,p_container,p_retailprice,p_comment', 2, 2);
insert into catalog.objects values(5, 'partsupp', 'ps_partkey,ps_suppkey,ps_availqty,ps_supplycost,ps_comment', 2, 2);
insert into catalog.objects values(6, 'region', 'r_regionkey,r_name,r_comment', 2, 2);
insert into catalog.objects values(7, 'supplier', 's_suppkey,s_name,s_address,s_nationkey,s_phone,s_acctbal,s_comment', 2, 2);
