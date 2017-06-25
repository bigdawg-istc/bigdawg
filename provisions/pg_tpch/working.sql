\timing on
-- 1 works
select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= date '1998-12-01' - interval '65' day group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus LIMIT 1;
-- 12 works
SELECT lineitem.l_shipmode, sum(CASE WHEN (orders.o_orderpriority = '1-URGENT') OR (orders.o_orderpriority = '2-HIGH') THEN 1 ELSE 0 END) AS sumCASEWHENordersoor137, sum(CASE WHEN (orders.o_orderpriority <> '1-URGENT') AND (orders.o_orderpriority <> '2-HIGH') THEN 1 ELSE 0 END) AS sumCASEWHENordersoor77 FROM lineitem, orders WHERE ((lineitem.l_commitdate < lineitem.l_receiptdate) AND (lineitem.l_shipdate < lineitem.l_commitdate) AND (lineitem.l_receiptdate >= date '1994-01-01') AND (lineitem.l_receiptdate < date '1994-01-01' + interval '1' year)) AND (orders.o_orderkey = lineitem.l_orderkey) GROUP BY lineitem.l_shipmode ORDER BY lineitem.l_shipmode;
-- 18 works w/o order by
select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) from customer, orders, lineitem where o_orderkey in ( select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > 315 ) and c_custkey = o_custkey and o_orderkey = l_orderkey group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice LIMIT 100;
-- 3 w/o order by
select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority from customer, orders, lineitem where c_mktsegment = 'MACHINERY' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < date '1995-03-01' and l_shipdate > date '1995-03-01' group by l_orderkey, o_orderdate, o_shippriority LIMIT 10;
-- 5 w/o order by
select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue from customer, orders, lineitem, supplier, nation, region where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'MIDDLE EAST' and o_orderdate >= date '1993-01-01' and o_orderdate < date '1993-01-01' + interval '1' year group by n_name LIMIT 1;
-- 6
select sum(l_extendedprice * l_discount) as revenue from lineitem where l_shipdate >= date '1993-01-01' and l_shipdate < date '1993-01-01' + interval '1' year and l_discount between 0.03 - 0.01 and 0.03 + 0.01 and l_quantity < 25 LIMIT 1;
-- 14
select 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue from lineitem, part where l_partkey = p_partkey and l_shipdate >= date '1994-11-01' and l_shipdate < date '1994-11-01' + interval '1' month LIMIT 1;
