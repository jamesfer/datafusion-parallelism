-- SQLBench-H query 21 derived from TPC-H query 21 under the terms of the TPC Fair Use Policy.
-- TPC-H queries are Copyright 1993-2022 Transaction Processing Performance Council.
-- with a as (
    select
        o_orderstatus,
--         max(s_suppkey) as s_suppkey,
        max(c_nationkey) as c_nationkey,
        max(c_custkey) as c_custkey,
        max(n_nationkey) as n_nationkey,
        max(n_name) as n_name,
        max(o_custkey) as o_custkey,
--         max(o_orderkey) as o_orderkey,
--         max(l_suppkey) as l_suppkey,
--         max(l_orderkey) as l_orderkey,
        avg(o_totalprice) as avg_price
    from
--         supplier,
--         lineitem l1,
        orders,
        customer,
        nation
    where
--       s_nationkey = n_nationkey
--       and s_suppkey = l1.l_suppkey
--       and o_orderkey = l1.l_orderkey
--       and o_orderstatus = 'F'
--       and l1.l_receiptdate > l1.l_commitdate
      c_custkey = o_custkey
      and c_nationkey = n_nationkey
      and n_name = 'ARGENTINA'
    group by
        1
-- )
-- select
--     min(a.s_name),
--     avg(a.numwait)
-- from a;
