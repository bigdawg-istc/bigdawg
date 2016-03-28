TBL_LOCATION=$1;

# drop existing array that carries prior final product
# NOTE: errors are suppressed
iquery -q "drop array region;
drop array nation;
drop array part;
drop array supplier;
drop array partsupp;
drop array customer;
drop array orders;
drop array lineitem;" > /dev/null 2>&1

# create the flat versions of all arrays
iquery -aq "
create array region_flat <r_regionkey:int64,r_name:string,r_comment:string>[i=0:*,1000000,0];
create array nation_flat <n_nationkey:int64,n_name:string,n_regionkey:int64,n_comment:string>[i=0:*,1000000,0];
create array part_flat <p_partkey:int64,p_name:string,p_mfgr:string,p_brand:string,p_type:string,p_size:int64,p_container:string,p_retailprice:double,p_comment:string>[i=0:*,1000000,0];
create array supplier_flat <s_suppkey:int64,s_name:string,s_address:string,s_nationkey:int64,s_phone:string,s_acctbal:double,s_comment:string>[i=0:*,1000000,0];
create array partsupp_flat <ps_partkey:int64,ps_suppkey:int64,ps_availqty:int64,ps_supplycost:double,ps_comment:string>[i=0:*,1000000,0];
create array customer_flat <c_custkey:int64,c_name:string,c_address:string,c_nationkey:int64,c_phone:string,c_acctbal:double,c_mktsegment:string,c_comment:string>[i=0:*,1000000,0];
create array orders_flat <o_orderkey:int64,o_custkey:int64,o_orderstatus:string,o_totalprice:double,o_orderdate:datetime,o_orderpriority:string,o_clerk:string,o_shippriority:int64,o_comment:string>[i=0:*,1000000,0];
create array lineitem_flat <l_orderkey:int64,l_partkey:int64,l_suppkey:int64,l_linenumber:int64,l_quantity:double,l_extendedprice:double,l_discount:double,l_tax:double,l_returnflag:string,l_linestatus:string,l_shipdate:datetime,l_commitdate:datetime,l_receiptdate:datetime,l_shipinstruct:string,l_shipmode:string,l_comment:string>[i=0:*,1000000,0];" > /dev/null

# load all data into the flat arrays
iquery -aq "load(region_flat, '${TBL_LOCATION}/region.tbl', -2, 'csv:p',1);" > /dev/null;
echo "'region_flat' loaded"
iquery -aq "load(nation_flat, '${TBL_LOCATION}/nation.tbl', -2, 'csv:p',1);" > /dev/null;
echo "'nation_flat' loaded"
iquery -aq "load(part_flat, '${TBL_LOCATION}/part.tbl', -2, 'csv:p',1);" > /dev/null;
echo "'part_flat' loaded"
iquery -aq "load(supplier_flat, '${TBL_LOCATION}/supplier.tbl', -2, 'csv:p',1);" > /dev/null;
echo "'supplier_flat' loaded"
iquery -aq "load(partsupp_flat, '${TBL_LOCATION}/partsupp.tbl', -2, 'csv:p',1);" > /dev/null;
echo "'partsupp_flat' loaded"
iquery -aq "load(customer_flat, '${TBL_LOCATION}/customer.tbl', -2, 'csv:p',1);" > /dev/null;
echo "'customer_flat' loaded"
iquery -aq "load(orders_flat, '${TBL_LOCATION}/orders.tbl', -2, 'csv:p',1);" > /dev/null;
echo "'orders_flat' loaded"
iquery -aq "load(lineitem_flat, '${TBL_LOCATION}/lineitem.tbl', -2, 'csv:p',1);" > /dev/null;
echo "'lineitem_flat' loaded"

# create arrays that hold final products
iquery -aq "create array region <r_name:string,r_comment:string>[r_regionkey=0:*,1000,0];
create array nation <n_name:string,n_comment:string>[n_nationkey=0:*,1000,0,n_regionkey=0:*,1000,0];
create array part <p_name:string,p_mfgr:string,p_brand:string,p_type:string,p_size:int64,p_container:string,p_retailprice:double,p_comment:string>[p_partkey=0:*,1000,0];
create array supplier <s_name:string,s_address:string,s_phone:string,s_acctbal:double,s_comment:string>[s_suppkey=0:*,1000,0,s_nationkey=0:*,1000,0];
create array partsupp <ps_availqty:int64,ps_supplycost:double,ps_comment:string>[ps_partkey=0:*,1000,0,ps_suppkey=0:*,1000,0];
create array customer <c_name:string,c_address:string,c_phone:string,c_acctbal:double,c_mktsegment:string,c_comment:string>[c_custkey=0:*,1000,0,c_nationkey=0:*,1000,0];
create array orders <o_orderstatus:string,o_totalprice:double,o_orderdate:datetime,o_orderpriority:string,o_clerk:string,o_shippriority:int64,o_comment:string>[o_orderkey=0:*,125,0,o_custkey=0:*,125,0];
create array lineitem <l_linenumber:int64,l_quantity:double,l_extendedprice:double,l_discount:double,l_tax:double,l_returnflag:string,l_linestatus:string,l_shipdate:datetime,l_commitdate:datetime,l_receiptdate:datetime,l_shipinstruct:string,l_shipmode:string,l_comment:string>[l_orderkey=0:*,25,0,l_partkey=0:*,25,0,l_suppkey=0:*,25,0];" > /dev/null

# redimensioning arrays
iquery -aq "store(redimension(region_flat,region), region);" > /dev/null
echo "'region' redimension completed"
iquery -aq "store(redimension(nation_flat,nation), nation);" > /dev/null
echo "'nation' redimension completed"
iquery -aq "store(redimension(part_flat,part), part);" > /dev/null
echo "'part' redimension completed"
iquery -aq "store(redimension(supplier_flat,supplier), supplier);" > /dev/null
echo "'supplier' redimension completed"
iquery -aq "store(redimension(partsupp_flat,partsupp), partsupp);" > /dev/null
echo "'partsupp' redimension completed"
iquery -aq "store(redimension(customer_flat,customer), customer);" > /dev/null
echo "'customer' redimension completed"
iquery -aq "store(redimension(orders_flat,orders), orders);" > /dev/null
echo "'orders' redimension completed"
iquery -aq "store(redimension(lineitem_flat,lineitem), lineitem);" > /dev/null
echo "'lineitem' redimension completed"

# drop the flat (temporary) arrays
iquery -q "
DROP ARRAY region_flat; 
DROP ARRAY nation_flat;
DROP ARRAY part_flat;
DROP ARRAY supplier_flat;
DROP ARRAY partsupp_flat;
DROP ARRAY customer_flat;
DROP ARRAY orders_flat;
DROP ARRAY lineitem_flat;" > /dev/null

echo "Fin!"