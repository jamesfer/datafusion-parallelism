-- SQLBench-H query 21 derived from TPC-H query 21 under the terms of the TPC Fair Use Policy.
-- TPC-H queries are Copyright 1993-2022 Transaction Processing Performance Council.
-- with a as (
    select
        s_name,
        max(s_suppkey) as s_suppkey,
        max(s_nationkey) as s_nationkey,
        max(n_nationkey) as n_nationkey,
        max(o_orderkey) as o_orderkey,
        max(l_suppkey) as l_suppkey,
        max(l_orderkey) as l_orderkey,
        count(*) as numwait
    from supplier,
        lineitem l1,
        orders,
        nation
    where s_suppkey = l1.l_suppkey
      and o_orderkey = l1.l_orderkey
      and o_orderstatus = 'F'
      and l1.l_receiptdate > l1.l_commitdate
      and s_nationkey = n_nationkey
      and n_name = 'ARGENTINA'
    group by
        s_name
-- )
-- select
--     min(a.s_name),
--     avg(a.numwait)
-- from a;
