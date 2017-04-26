import os
import csv
from datetime import datetime, time
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

#data_path = "/home/cohara/code/dataalgebradata/tpch/10G/16/"
data_path = "/home/cohara/code/dataalgebradata/tpch/1M/4/"
# create dictionary and list of table names
data_frames = {}
tables = ["customer", "orders", "lineitem", "supplier", "nation", "region", "part", "partsupp"]

# function that creates dataFrames and tempTables, appending to our dictionary and iterating table elements
def return_dataframe():
    for table in tables:
        # create df
        data_frames["df_" + table] = spark.read.parquet(data_path + table + ".parquet")
        # register tempTables
        data_frames["df_" + table].registerTempTable(table)


# calling our function to load tables
return_dataframe()

# init vars
runtimes = []


def run_benchmark_query(query, message):
    message = "[QUERY " + message.strip() +"]"

    print("Starting: " + message)

    query_start_time = datetime.now()
    df = spark.sql(query)
    df.show(100)
    query_stop_time = datetime.now()
    run_time = (query_stop_time-query_start_time).seconds
    runtimes.append(run_time)

    print("Runtime: %s seconds" % run_time)


# Establishing the queries from the TPCH query set for purposes of demonstration. Notice the variety and increasing complexity from the prior notebook.

#======Round 1========

#TPCH Query 3
#===================================
run_benchmark_query(
    '''
    select
        l_shipmode,
        sum(case
            when o_orderpriority = '1-URGENT'
                or o_orderpriority = '2-HIGH'
                then 1
            else 0
        end) as high_line_count,
        sum(case
            when o_orderpriority <> '1-URGENT'
                and o_orderpriority <> '2-HIGH'
                then 1
            else 0
        end) as low_line_count
    from
        orders,
        lineitem
    where
        o_orderkey = l_orderkey
        and l_shipmode in ('FOB', 'REG AIR')
        and l_commitdate < l_receiptdate
        and l_shipdate < l_commitdate
        and l_receiptdate >= cast('1993-01-01'as date)
        and l_receiptdate < add_months(cast('1993-01-01'as date), '12')
    group by
        l_shipmode
    order by
        l_shipmode
    ''',
    '''
    q12_10_1
    '''
)

run_benchmark_query(
    '''
    select
        l_shipmode,
        sum(case
            when o_orderpriority = '1-URGENT'
                or o_orderpriority = '2-HIGH'
                then 1
            else 0
        end) as high_line_count,
        sum(case
            when o_orderpriority <> '1-URGENT'
                and o_orderpriority <> '2-HIGH'
                then 1
            else 0
        end) as low_line_count
    from
        orders,
        lineitem
    where
        o_orderkey = l_orderkey
        and l_shipmode in ('MAIL', 'REG AIR')
        and l_commitdate < l_receiptdate
        and l_shipdate < l_commitdate
        and l_receiptdate >= cast('1993-01-01'as date)
        and l_receiptdate < add_months(cast('1993-01-01'as date), '12')
    group by
        l_shipmode
    order by
        l_shipmode
    ''',
    '''
    q12_10_2
    '''
)

run_benchmark_query(
    '''
    select
        l_shipmode,
        sum(case
            when o_orderpriority = '1-URGENT'
                or o_orderpriority = '2-HIGH'
                then 1
            else 0
        end) as high_line_count,
        sum(case
            when o_orderpriority <> '1-URGENT'
                and o_orderpriority <> '2-HIGH'
                then 1
            else 0
        end) as low_line_count
    from
        orders,
        lineitem
    where
        o_orderkey = l_orderkey
        and l_shipmode in ('TRUCK', 'REG AIR')
        and l_commitdate < l_receiptdate
        and l_shipdate < l_commitdate
        and l_receiptdate >= cast('1993-01-01'as date)
        and l_receiptdate < add_months(cast('1993-01-01'as date), '12')
    group by
        l_shipmode
    order by
        l_shipmode
    ''',
    '''
    q12_10_3
    '''
)

run_benchmark_query(
    '''
    select
        l_shipmode,
        sum(case
            when o_orderpriority = '1-URGENT'
                or o_orderpriority = '2-HIGH'
                then 1
            else 0
        end) as high_line_count,
        sum(case
            when o_orderpriority <> '1-URGENT'
                and o_orderpriority <> '2-HIGH'
                then 1
            else 0
        end) as low_line_count
    from
        orders,
        lineitem
    where
        o_orderkey = l_orderkey
        and l_shipmode in ('RAIL', 'REG AIR')
        and l_commitdate < l_receiptdate
        and l_shipdate < l_commitdate
        and l_receiptdate >= cast('1994-01-01'as date)
        and l_receiptdate < add_months(cast('1994-01-01'as date), '12')
    group by
        l_shipmode
    order by
        l_shipmode
    ''',
    '''
    q12_10_4
    '''
)

run_benchmark_query(
    '''
    select
        l_shipmode,
        sum(case
            when o_orderpriority = '1-URGENT'
                or o_orderpriority = '2-HIGH'
                then 1
            else 0
        end) as high_line_count,
        sum(case
            when o_orderpriority <> '1-URGENT'
                and o_orderpriority <> '2-HIGH'
                then 1
            else 0
        end) as low_line_count
    from
        orders,
        lineitem
    where
        o_orderkey = l_orderkey
        and l_shipmode in ('AIR', 'MAIL')
        and l_commitdate < l_receiptdate
        and l_shipdate < l_commitdate
        and l_receiptdate >= cast('1994-01-01'as date)
        and l_receiptdate < add_months(cast('1994-01-01'as date), '12')
    group by
        l_shipmode
    order by
        l_shipmode
    ''',
    '''
    q12_10_5
    '''
)

run_benchmark_query(
    '''
    select
        l_shipmode,
        sum(case
            when o_orderpriority = '1-URGENT'
                or o_orderpriority = '2-HIGH'
                then 1
            else 0
        end) as high_line_count,
        sum(case
            when o_orderpriority <> '1-URGENT'
                and o_orderpriority <> '2-HIGH'
                then 1
            else 0
        end) as low_line_count
    from
        orders,
        lineitem
    where
        o_orderkey = l_orderkey
        and l_shipmode in ('SHIP', 'AIR')
        and l_commitdate < l_receiptdate
        and l_shipdate < l_commitdate
        and l_receiptdate >= cast('1994-01-01'as date)
        and l_receiptdate < add_months(cast('1994-01-01'as date), '12')
    group by
        l_shipmode
    order by
        l_shipmode
    ''',
    '''
    q12_10_6
    '''
)

run_benchmark_query(
    '''
    select
        l_shipmode,
        sum(case
            when o_orderpriority = '1-URGENT'
                or o_orderpriority = '2-HIGH'
                then 1
            else 0
        end) as high_line_count,
        sum(case
            when o_orderpriority <> '1-URGENT'
                and o_orderpriority <> '2-HIGH'
                then 1
            else 0
        end) as low_line_count
    from
        orders,
        lineitem
    where
        o_orderkey = l_orderkey
        and l_shipmode in ('FOB', 'AIR')
        and l_commitdate < l_receiptdate
        and l_shipdate < l_commitdate
        and l_receiptdate >= cast('1994-01-01'as date)
        and l_receiptdate < add_months(cast('1994-01-01'as date), '12')
    group by
        l_shipmode
    order by
        l_shipmode
    ''',
    '''
    q12_10_7
    '''
)

run_benchmark_query(
    '''
    select
        l_shipmode,
        sum(case
            when o_orderpriority = '1-URGENT'
                or o_orderpriority = '2-HIGH'
                then 1
            else 0
        end) as high_line_count,
        sum(case
            when o_orderpriority <> '1-URGENT'
                and o_orderpriority <> '2-HIGH'
                then 1
            else 0
        end) as low_line_count
    from
        orders,
        lineitem
    where
        o_orderkey = l_orderkey
        and l_shipmode in ('MAIL', 'AIR')
        and l_commitdate < l_receiptdate
        and l_shipdate < l_commitdate
        and l_receiptdate >= cast('1995-01-01'as date)
        and l_receiptdate < add_months(cast('1995-01-01'as date), '12')
    group by
        l_shipmode
    order by
        l_shipmode
    ''',
    '''
    q12_10_8
    '''
)

run_benchmark_query(
    '''
    select
        l_shipmode,
        sum(case
            when o_orderpriority = '1-URGENT'
                or o_orderpriority = '2-HIGH'
                then 1
            else 0
        end) as high_line_count,
        sum(case
            when o_orderpriority <> '1-URGENT'
                and o_orderpriority <> '2-HIGH'
                then 1
            else 0
        end) as low_line_count
    from
        orders,
        lineitem
    where
        o_orderkey = l_orderkey
        and l_shipmode in ('TRUCK', 'RAIL')
        and l_commitdate < l_receiptdate
        and l_shipdate < l_commitdate
        and l_receiptdate >= cast('1995-01-01'as date)
        and l_receiptdate < add_months(cast('1995-01-01'as date), '12')
    group by
        l_shipmode
    order by
        l_shipmode
    ''',
    '''
    q12_10_9
    '''
)

run_benchmark_query(
    '''
    select
        l_shipmode,
        sum(case
            when o_orderpriority = '1-URGENT'
                or o_orderpriority = '2-HIGH'
                then 1
            else 0
        end) as high_line_count,
        sum(case
            when o_orderpriority <> '1-URGENT'
                and o_orderpriority <> '2-HIGH'
                then 1
            else 0
        end) as low_line_count
    from
        orders,
        lineitem
    where
        o_orderkey = l_orderkey
        and l_shipmode in ('RAIL', 'TRUCK')
        and l_commitdate < l_receiptdate
        and l_shipdate < l_commitdate
        and l_receiptdate >= cast('1994-01-01'as date)
        and l_receiptdate < add_months(cast('1994-01-01'as date), '12')
    group by
        l_shipmode
    order by
        l_shipmode
    ''',
    '''
    q12_10_10
    '''
)

run_benchmark_query(
    '''
    select
        n_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue
    from
        customer,
        orders,
        lineitem,
        supplier,
        nation,
        region
    where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and l_suppkey = s_suppkey
        and c_nationkey = s_nationkey
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'AFRICA'
        and o_orderdate >= cast('1993-01-01' as date)
        and o_orderdate < add_months(cast('1993-01-01' as date), '12')
    group by
        n_name
    order by
        revenue desc
    ''',
    '''
    q5_10_1
    '''
)

run_benchmark_query(
    '''
    select
        n_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue
    from
        customer,
        orders,
        lineitem,
        supplier,
        nation,
        region
    where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and l_suppkey = s_suppkey
        and c_nationkey = s_nationkey
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'AMERICA'
        and o_orderdate >= cast('1993-01-01' as date)
        and o_orderdate < add_months(cast('1993-01-01' as date), '12')
    group by
        n_name
    order by
        revenue desc
    ''',
    '''
    q5_10_2
    '''
)

run_benchmark_query(
    '''
    select
        n_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue
    from
        customer,
        orders,
        lineitem,
        supplier,
        nation,
        region
    where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and l_suppkey = s_suppkey
        and c_nationkey = s_nationkey
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'ASIA'
        and o_orderdate >= cast('1993-01-01' as date)
        and o_orderdate < add_months(cast('1993-01-01' as date), '12')
    group by
        n_name
    order by
        revenue desc
    ''',
    '''
    q5_10_3
    '''
)

run_benchmark_query(
    '''
    select
        n_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue
    from
        customer,
        orders,
        lineitem,
        supplier,
        nation,
        region
    where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and l_suppkey = s_suppkey
        and c_nationkey = s_nationkey
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'EUROPE'
        and o_orderdate >= cast('1993-01-01' as date)
        and o_orderdate < add_months(cast('1993-01-01' as date), '12')
    group by
        n_name
    order by
        revenue desc
    ''',
    '''
    q5_10_4
    '''
)

run_benchmark_query(
    '''
    select
        n_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue
    from
        customer,
        orders,
        lineitem,
        supplier,
        nation,
        region
    where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and l_suppkey = s_suppkey
        and c_nationkey = s_nationkey
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'MIDDLE EAST'
        and o_orderdate >= cast('1993-01-01' as date)
        and o_orderdate < add_months(cast('1993-01-01' as date), '12')
    group by
        n_name
    order by
        revenue desc
    ''',
    '''
    q5_10_5
    '''
)

run_benchmark_query(
    '''
    select
        n_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue
    from
        customer,
        orders,
        lineitem,
        supplier,
        nation,
        region
    where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and l_suppkey = s_suppkey
        and c_nationkey = s_nationkey
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'AFRICA'
        and o_orderdate >= cast('1994-01-01' as date)
        and o_orderdate < add_months(cast('1994-01-01' as date), '12')
    group by
        n_name
    order by
        revenue desc
    ''',
    '''
    q5_10_6
    '''
)

run_benchmark_query(
    '''
    select
        n_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue
    from
        customer,
        orders,
        lineitem,
        supplier,
        nation,
        region
    where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and l_suppkey = s_suppkey
        and c_nationkey = s_nationkey
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'AMERICA'
        and o_orderdate >= cast('1994-01-01' as date)
        and o_orderdate < add_months(cast('1994-01-01' as date), '12')
    group by
        n_name
    order by
        revenue desc
    ''',
    '''
    q5_10_7
    '''
)

run_benchmark_query(
    '''
    select
        n_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue
    from
        customer,
        orders,
        lineitem,
        supplier,
        nation,
        region
    where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and l_suppkey = s_suppkey
        and c_nationkey = s_nationkey
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'ASIA'
        and o_orderdate >= cast('1994-01-01' as date)
        and o_orderdate < add_months(cast('1994-01-01' as date), '12')
    group by
        n_name
    order by
        revenue desc
    ''',
    '''
    q5_10_8
    '''
)

run_benchmark_query(
    '''
    select
        n_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue
    from
        customer,
        orders,
        lineitem,
        supplier,
        nation,
        region
    where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and l_suppkey = s_suppkey
        and c_nationkey = s_nationkey
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'EUROPE'
        and o_orderdate >= cast('1994-01-01' as date)
        and o_orderdate < add_months(cast('1994-01-01' as date), '12')
    group by
        n_name
    order by
        revenue desc
    ''',
    '''
    q5_10_9
    '''
)

run_benchmark_query(
    '''
    select
        n_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue
    from
        customer,
        orders,
        lineitem,
        supplier,
        nation,
        region
    where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and l_suppkey = s_suppkey
        and c_nationkey = s_nationkey
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'MIDDLE EAST'
        and o_orderdate >= cast('1995-01-01' as date)
        and o_orderdate < add_months(cast('1995-01-01' as date), '12')
    group by
        n_name
    order by
        revenue desc
    ''',
    '''
    q5_10_10
    '''
)

run_benchmark_query(
    '''
    select
        o_year,
        sum(case
            when nation = 'ALGERIA' then volume
            else 0
        end) / sum(volume) as mkt_share
    from
        (
            select
                year(o_orderdate) as o_year,
                l_extendedprice * (1 - l_discount) as volume,
                n2.n_name as nation
            from
                part,
                supplier,
                lineitem,
                orders,
                customer,
                nation n1,
                nation n2,
                region
            where
                p_partkey = l_partkey
                and s_suppkey = l_suppkey
                and l_orderkey = o_orderkey
                and o_custkey = c_custkey
                and c_nationkey = n1.n_nationkey
                and n1.n_regionkey = r_regionkey
                and r_name = 'AFRICA'
                and s_nationkey = n2.n_nationkey
                and o_orderdate between cast('1995-01-01'as date) and cast('1996-12-31'as date)
                and p_type = 'STANDARD ANODIZED TIN'
        ) as all_nations
    group by
        o_year
    order by
        o_year
    ''',
    '''
    q8_10_1
    '''
)

run_benchmark_query(
    '''
    select
        o_year,
        sum(case
            when nation = 'MOZAMBIQUE' then volume
            else 0
        end) / sum(volume) as mkt_share
    from
        (
            select
                year(o_orderdate) as o_year,
                l_extendedprice * (1 - l_discount) as volume,
                n2.n_name as nation
            from
                part,
                supplier,
                lineitem,
                orders,
                customer,
                nation n1,
                nation n2,
                region
            where
                p_partkey = l_partkey
                and s_suppkey = l_suppkey
                and l_orderkey = o_orderkey
                and o_custkey = c_custkey
                and c_nationkey = n1.n_nationkey
                and n1.n_regionkey = r_regionkey
                and r_name = 'AFRICA'
                and s_nationkey = n2.n_nationkey
                and o_orderdate between cast('1995-01-01'as date) and cast('1996-12-31'as date)
                and p_type = 'PROMO POLISHED TIN'
        ) as all_nations
    group by
        o_year
    order by
        o_year
    ''',
    '''
    q8_10_2
    '''
)

run_benchmark_query(
    '''
    select
        o_year,
        sum(case
            when nation = 'INDIA' then volume
            else 0
        end) / sum(volume) as mkt_share
    from
        (
            select
                year(o_orderdate) as o_year,
                l_extendedprice * (1 - l_discount) as volume,
                n2.n_name as nation
            from
                part,
                supplier,
                lineitem,
                orders,
                customer,
                nation n1,
                nation n2,
                region
            where
                p_partkey = l_partkey
                and s_suppkey = l_suppkey
                and l_orderkey = o_orderkey
                and o_custkey = c_custkey
                and c_nationkey = n1.n_nationkey
                and n1.n_regionkey = r_regionkey
                and r_name = 'ASIA'
                and s_nationkey = n2.n_nationkey
                and o_orderdate between cast('1995-01-01'as date) and cast('1996-12-31'as date)
                and p_type = 'PROMO BURNISHED TIN'
        ) as all_nations
    group by
        o_year
    order by
        o_year
    ''',
    '''
    q8_10_3
    '''
)

run_benchmark_query(
    '''
    select
        o_year,
        sum(case
            when nation = 'ALGERIA' then volume
            else 0
        end) / sum(volume) as mkt_share
    from
        (
            select
                year(o_orderdate) as o_year,
                l_extendedprice * (1 - l_discount) as volume,
                n2.n_name as nation
            from
                part,
                supplier,
                lineitem,
                orders,
                customer,
                nation n1,
                nation n2,
                region
            where
                p_partkey = l_partkey
                and s_suppkey = l_suppkey
                and l_orderkey = o_orderkey
                and o_custkey = c_custkey
                and c_nationkey = n1.n_nationkey
                and n1.n_regionkey = r_regionkey
                and r_name = 'AFRICA'
                and s_nationkey = n2.n_nationkey
                and o_orderdate between cast('1995-01-01'as date) and cast('1996-12-31'as date)
                and p_type = 'ECONOMY BRUSHED TIN'
        ) as all_nations
    group by
        o_year
    order by
        o_year
    ''',
    '''
    q8_10_4
    '''
)

run_benchmark_query(
    '''
    select
        o_year,
        sum(case
            when nation = 'PERU' then volume
            else 0
        end) / sum(volume) as mkt_share
    from
        (
            select
                year(o_orderdate) as o_year,
                l_extendedprice * (1 - l_discount) as volume,
                n2.n_name as nation
            from
                part,
                supplier,
                lineitem,
                orders,
                customer,
                nation n1,
                nation n2,
                region
            where
                p_partkey = l_partkey
                and s_suppkey = l_suppkey
                and l_orderkey = o_orderkey
                and o_custkey = c_custkey
                and c_nationkey = n1.n_nationkey
                and n1.n_regionkey = r_regionkey
                and r_name = 'AMERICA'
                and s_nationkey = n2.n_nationkey
                and o_orderdate between cast('1995-01-01'as date) and cast('1996-12-31'as date)
                and p_type = 'ECONOMY PLATED TIN'
        ) as all_nations
    group by
        o_year
    order by
        o_year
    ''',
    '''
    q8_10_5
    '''
)

run_benchmark_query(
    '''
    select
        o_year,
        sum(case
            when nation = 'INDONESIA' then volume
            else 0
        end) / sum(volume) as mkt_share
    from
        (
            select
                year(o_orderdate) as o_year,
                l_extendedprice * (1 - l_discount) as volume,
                n2.n_name as nation
            from
                part,
                supplier,
                lineitem,
                orders,
                customer,
                nation n1,
                nation n2,
                region
            where
                p_partkey = l_partkey
                and s_suppkey = l_suppkey
                and l_orderkey = o_orderkey
                and o_custkey = c_custkey
                and c_nationkey = n1.n_nationkey
                and n1.n_regionkey = r_regionkey
                and r_name = 'ASIA'
                and s_nationkey = n2.n_nationkey
                and o_orderdate between cast('1995-01-01'as date) and cast('1996-12-31'as date)
                and p_type = 'ECONOMY ANODIZED NICKEL'
        ) as all_nations
    group by
        o_year
    order by
        o_year
    ''',
    '''
    q8_10_6
    '''
)

run_benchmark_query(
    '''
    select
        o_year,
        sum(case
            when nation = 'ARGENTINA' then volume
            else 0
        end) / sum(volume) as mkt_share
    from
        (
            select
                year(o_orderdate) as o_year,
                l_extendedprice * (1 - l_discount) as volume,
                n2.n_name as nation
            from
                part,
                supplier,
                lineitem,
                orders,
                customer,
                nation n1,
                nation n2,
                region
            where
                p_partkey = l_partkey
                and s_suppkey = l_suppkey
                and l_orderkey = o_orderkey
                and o_custkey = c_custkey
                and c_nationkey = n1.n_nationkey
                and n1.n_regionkey = r_regionkey
                and r_name = 'AMERICA'
                and s_nationkey = n2.n_nationkey
                and o_orderdate between cast('1995-01-01'as date) and cast('1996-12-31'as date)
                and p_type = 'LARGE POLISHED NICKEL'
        ) as all_nations
    group by
        o_year
    order by
        o_year
    ''',
    '''
    q8_10_7
    '''
)

run_benchmark_query(
    '''
    select
        o_year,
        sum(case
            when nation = 'CHINA' then volume
            else 0
        end) / sum(volume) as mkt_share
    from
        (
            select
                year(o_orderdate) as o_year,
                l_extendedprice * (1 - l_discount) as volume,
                n2.n_name as nation
            from
                part,
                supplier,
                lineitem,
                orders,
                customer,
                nation n1,
                nation n2,
                region
            where
                p_partkey = l_partkey
                and s_suppkey = l_suppkey
                and l_orderkey = o_orderkey
                and o_custkey = c_custkey
                and c_nationkey = n1.n_nationkey
                and n1.n_regionkey = r_regionkey
                and r_name = 'ASIA'
                and s_nationkey = n2.n_nationkey
                and o_orderdate between cast('1995-01-01'as date) and cast('1996-12-31'as date)
                and p_type = 'LARGE BURNISHED NICKEL'
        ) as all_nations
    group by
        o_year
    order by
        o_year
    ''',
    '''
    q8_10_8
    '''
)

run_benchmark_query(
    '''
    select
        o_year,
        sum(case
            when nation = 'IRAN' then volume
            else 0
        end) / sum(volume) as mkt_share
    from
        (
            select
                year(o_orderdate) as o_year,
                l_extendedprice * (1 - l_discount) as volume,
                n2.n_name as nation
            from
                part,
                supplier,
                lineitem,
                orders,
                customer,
                nation n1,
                nation n2,
                region
            where
                p_partkey = l_partkey
                and s_suppkey = l_suppkey
                and l_orderkey = o_orderkey
                and o_custkey = c_custkey
                and c_nationkey = n1.n_nationkey
                and n1.n_regionkey = r_regionkey
                and r_name = 'MIDDLE EAST'
                and s_nationkey = n2.n_nationkey
                and o_orderdate between cast('1995-01-01'as date) and cast('1996-12-31'as date)
                and p_type = 'MEDIUM BRUSHED NICKEL'
        ) as all_nations
    group by
        o_year
    order by
        o_year
    ''',
    '''
    q8_10_9
    '''
)

run_benchmark_query(
    '''
    select
        o_year,
        sum(case
            when nation = 'BRAZIL' then volume
            else 0
        end) / sum(volume) as mkt_share
    from
        (
            select
                year(o_orderdate) as o_year,
                l_extendedprice * (1 - l_discount) as volume,
                n2.n_name as nation
            from
                part,
                supplier,
                lineitem,
                orders,
                customer,
                nation n1,
                nation n2,
                region
            where
                p_partkey = l_partkey
                and s_suppkey = l_suppkey
                and l_orderkey = o_orderkey
                and o_custkey = c_custkey
                and c_nationkey = n1.n_nationkey
                and n1.n_regionkey = r_regionkey
                and r_name = 'AMERICA'
                and s_nationkey = n2.n_nationkey
                and o_orderdate between cast('1995-01-01'as date) and cast('1996-12-31'as date)
                and p_type = 'MEDIUM PLATED NICKEL'
        ) as all_nations
    group by
        o_year
    order by
        o_year
    ''',
    '''
    q8_10_10
    '''
)

run_benchmark_query(
    '''
    select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
    from
        lineitem
    where
        l_shipdate <= date_sub(cast('1998-12-01' as date), '60')
    group by
        l_returnflag,
        l_linestatus
    order by
        l_returnflag,
        l_linestatus
    ''',
    '''
    q1_10_1
    '''
)

run_benchmark_query(
    '''
    select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
    from
        lineitem
    where
        l_shipdate <= date_sub(cast('1998-12-01' as date), '68')
    group by
        l_returnflag,
        l_linestatus
    order by
        l_returnflag,
        l_linestatus
    ''',
    '''
    q1_10_2
    '''
)

run_benchmark_query(
    '''
    select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
    from
        lineitem
    where
        l_shipdate <= date_sub(cast('1998-12-01' as date), '76')
    group by
        l_returnflag,
        l_linestatus
    order by
        l_returnflag,
        l_linestatus
    ''',
    '''
    q1_10_3
    '''
)

run_benchmark_query(
    '''
    select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
    from
        lineitem
    where
        l_shipdate <= date_sub(cast('1998-12-01' as date), '84')
    group by
        l_returnflag,
        l_linestatus
    order by
        l_returnflag,
        l_linestatus
    ''',
    '''
    q1_10_4
    '''
)

run_benchmark_query(
    '''
    select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
    from
        lineitem
    where
        l_shipdate <= date_sub(cast('1998-12-01' as date), '92')
    group by
        l_returnflag,
        l_linestatus
    order by
        l_returnflag,
        l_linestatus
    ''',
    '''
    q1_10_5
    '''
)

run_benchmark_query(
    '''
    select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
    from
        lineitem
    where
        l_shipdate <= date_sub(cast('1998-12-01' as date), '100')
    group by
        l_returnflag,
        l_linestatus
    order by
        l_returnflag,
        l_linestatus
    ''',
    '''
    q1_10_6
    '''
)

run_benchmark_query(
    '''
    select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
    from
        lineitem
    where
        l_shipdate <= date_sub(cast('1998-12-01' as date), '108')
    group by
        l_returnflag,
        l_linestatus
    order by
        l_returnflag,
        l_linestatus
    ''',
    '''
    q1_10_7
    '''
)

run_benchmark_query(
    '''
    select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
    from
        lineitem
    where
        l_shipdate <= date_sub(cast('1998-12-01' as date), '116')
    group by
        l_returnflag,
        l_linestatus
    order by
        l_returnflag,
        l_linestatus
    ''',
    '''
    q1_10_8
    '''
)

run_benchmark_query(
    '''
    select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
    from
        lineitem
    where
        l_shipdate <= date_sub(cast('1998-12-01' as date), '63')
    group by
        l_returnflag,
        l_linestatus
    order by
        l_returnflag,
        l_linestatus
    ''',
    '''
    q1_10_9
    '''
)

run_benchmark_query(
    '''
    select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
    from
        lineitem
    where
        l_shipdate <= date_sub(cast('1998-12-01' as date), '71')
    group by
        l_returnflag,
        l_linestatus
    order by
        l_returnflag,
        l_linestatus
    ''',
    '''
    q1_10_10
    '''
)

run_benchmark_query(
    '''
    select
        100.00 * sum(case
            when p_type like 'PROMO%'
                then l_extendedprice * (1 - l_discount)
            else 0
        end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
    from
        lineitem,
        part
    where
        l_partkey = p_partkey
        and l_shipdate >= cast('1993-02-01'as date)
        and l_shipdate < add_months(cast('1993-02-01'as date), '1')
    ''',
    '''
    q14_10_1
    '''
)

run_benchmark_query(
    '''
    select
        100.00 * sum(case
            when p_type like 'PROMO%'
                then l_extendedprice * (1 - l_discount)
            else 0
        end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
    from
        lineitem,
        part
    where
        l_partkey = p_partkey
        and l_shipdate >= cast('1993-05-01'as date)
        and l_shipdate < add_months(cast('1993-05-01'as date), '1')
    ''',
    '''
    q14_10_2
    '''
)

run_benchmark_query(
    '''
    select
        100.00 * sum(case
            when p_type like 'PROMO%'
                then l_extendedprice * (1 - l_discount)
            else 0
        end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
    from
        lineitem,
        part
    where
        l_partkey = p_partkey
        and l_shipdate >= cast('1993-08-01'as date)
        and l_shipdate < add_months(cast('1993-08-01'as date), '1')
    ''',
    '''
    q14_10_3
    '''
)

run_benchmark_query(
    '''
    select
        100.00 * sum(case
            when p_type like 'PROMO%'
                then l_extendedprice * (1 - l_discount)
            else 0
        end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
    from
        lineitem,
        part
    where
        l_partkey = p_partkey
        and l_shipdate >= cast('1993-11-01'as date)
        and l_shipdate < add_months(cast('1993-11-01'as date), '1')
    ''',
    '''
    q14_10_4
    '''
)

run_benchmark_query(
    '''
    select
        100.00 * sum(case
            when p_type like 'PROMO%'
                then l_extendedprice * (1 - l_discount)
            else 0
        end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
    from
        lineitem,
        part
    where
        l_partkey = p_partkey
        and l_shipdate >= cast('1994-02-01'as date)
        and l_shipdate < add_months(cast('1994-02-01'as date), '1')
    ''',
    '''
    q14_10_5
    '''
)

run_benchmark_query(
    '''
    select
        100.00 * sum(case
            when p_type like 'PROMO%'
                then l_extendedprice * (1 - l_discount)
            else 0
        end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
    from
        lineitem,
        part
    where
        l_partkey = p_partkey
        and l_shipdate >= cast('1994-06-01'as date)
        and l_shipdate < add_months(cast('1994-06-01'as date), '1')
    ''',
    '''
    q14_10_6
    '''
)

run_benchmark_query(
    '''
    select
        100.00 * sum(case
            when p_type like 'PROMO%'
                then l_extendedprice * (1 - l_discount)
            else 0
        end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
    from
        lineitem,
        part
    where
        l_partkey = p_partkey
        and l_shipdate >= cast('1994-09-01'as date)
        and l_shipdate < add_months(cast('1994-09-01'as date), '1')
    ''',
    '''
    q14_10_7
    '''
)

run_benchmark_query(
    '''
    select
        100.00 * sum(case
            when p_type like 'PROMO%'
                then l_extendedprice * (1 - l_discount)
            else 0
        end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
    from
        lineitem,
        part
    where
        l_partkey = p_partkey
        and l_shipdate >= cast('1994-12-01'as date)
        and l_shipdate < add_months(cast('1994-12-01'as date), '1')
    ''',
    '''
    q14_10_8
    '''
)

run_benchmark_query(
    '''
    select
        100.00 * sum(case
            when p_type like 'PROMO%'
                then l_extendedprice * (1 - l_discount)
            else 0
        end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
    from
        lineitem,
        part
    where
        l_partkey = p_partkey
        and l_shipdate >= cast('1995-03-01'as date)
        and l_shipdate < add_months(cast('1995-03-01'as date), '1')
    ''',
    '''
    q14_10_9
    '''
)

run_benchmark_query(
    '''
    select
        100.00 * sum(case
            when p_type like 'PROMO%'
                then l_extendedprice * (1 - l_discount)
            else 0
        end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
    from
        lineitem,
        part
    where
        l_partkey = p_partkey
        and l_shipdate >= cast('1995-06-01'as date)
        and l_shipdate < add_months(cast('1995-06-01'as date), '1')
    ''',
    '''
    q14_10_10
    '''
)

run_benchmark_query(
    '''
    select
        sum(l_extendedprice * l_discount) as revenue
    from
        lineitem
    where
        l_shipdate >= cast('1993-01-01' as date)
        and l_shipdate < add_months(cast('1993-01-01' as date), 12)
        and l_discount between 0.02 - 0.01 and 0.02 + 0.01
        and l_quantity < 24
    ''',
    '''
    q6_10_1
    '''
)

run_benchmark_query(
    '''
    select
        sum(l_extendedprice * l_discount) as revenue
    from
        lineitem
    where
        l_shipdate >= cast('1993-01-01' as date)
        and l_shipdate < add_months(cast('1993-01-01' as date), 12)
        and l_discount between 0.07 - 0.01 and 0.07 + 0.01
        and l_quantity < 25
    ''',
    '''
    q6_10_2
    '''
)

run_benchmark_query(
    '''
    select
        sum(l_extendedprice * l_discount) as revenue
    from
        lineitem
    where
        l_shipdate >= cast('1993-01-01' as date)
        and l_shipdate < add_months(cast('1993-01-01' as date), 12)
        and l_discount between 0.04 - 0.01 and 0.04 + 0.01
        and l_quantity < 24
    ''',
    '''
    q6_10_3
    '''
)

run_benchmark_query(
    '''
    select
        sum(l_extendedprice * l_discount) as revenue
    from
        lineitem
    where
        l_shipdate >= cast('1993-01-01' as date)
        and l_shipdate < add_months(cast('1993-01-01' as date), 12)
        and l_discount between 0.02 - 0.01 and 0.02 + 0.01
        and l_quantity < 24
    ''',
    '''
    q6_10_4
    '''
)

run_benchmark_query(
    '''
    select
        sum(l_extendedprice * l_discount) as revenue
    from
        lineitem
    where
        l_shipdate >= cast('1993-01-01' as date)
        and l_shipdate < add_months(cast('1993-01-01' as date), 12)
        and l_discount between 0.07 - 0.01 and 0.07 + 0.01
        and l_quantity < 25
    ''',
    '''
    q6_10_5
    '''
)

run_benchmark_query(
    '''
    select
        sum(l_extendedprice * l_discount) as revenue
    from
        lineitem
    where
        l_shipdate >= cast('1994-01-01' as date)
        and l_shipdate < add_months(cast('1994-01-01' as date), 12)
        and l_discount between 0.05 - 0.01 and 0.05 + 0.01
        and l_quantity < 24
    ''',
    '''
    q6_10_6
    '''
)

run_benchmark_query(
    '''
    select
        sum(l_extendedprice * l_discount) as revenue
    from
        lineitem
    where
        l_shipdate >= cast('1994-01-01' as date)
        and l_shipdate < add_months(cast('1994-01-01' as date), 12)
        and l_discount between 0.02 - 0.01 and 0.02 + 0.01
        and l_quantity < 24
    ''',
    '''
    q6_10_7
    '''
)

run_benchmark_query(
    '''
    select
        sum(l_extendedprice * l_discount) as revenue
    from
        lineitem
    where
        l_shipdate >= cast('1994-01-01' as date)
        and l_shipdate < add_months(cast('1994-01-01' as date), 12)
        and l_discount between 0.08 - 0.01 and 0.08 + 0.01
        and l_quantity < 25
    ''',
    '''
    q6_10_8
    '''
)

run_benchmark_query(
    '''
    select
        sum(l_extendedprice * l_discount) as revenue
    from
        lineitem
    where
        l_shipdate >= cast('1994-01-01' as date)
        and l_shipdate < add_months(cast('1994-01-01' as date), 12)
        and l_discount between 0.05 - 0.01 and 0.05 + 0.01
        and l_quantity < 24
    ''',
    '''
    q6_10_9
    '''
)

run_benchmark_query(
    '''
    select
        sum(l_extendedprice * l_discount) as revenue
    from
        lineitem
    where
        l_shipdate >= cast('1995-01-01' as date)
        and l_shipdate < add_months(cast('1995-01-01' as date), 12)
        and l_discount between 0.02 - 0.01 and 0.02 + 0.01
        and l_quantity < 24
    ''',
    '''
    q6_10_10
    '''
)

run_benchmark_query(
    '''
    select
        supp_nation,
        cust_nation,
        l_year,
        sum(volume) as revenue
    from
        (
            select
                n1.n_name as supp_nation,
                n2.n_name as cust_nation,
                year(l_shipdate) as l_year,
                l_extendedprice * (1 - l_discount) as volume
            from
                supplier,
                lineitem,
                orders,
                customer,
                nation n1,
                nation n2
            where
                s_suppkey = l_suppkey
                and o_orderkey = l_orderkey
                and c_custkey = o_custkey
                and s_nationkey = n1.n_nationkey
                and c_nationkey = n2.n_nationkey
                and (
                    (n1.n_name = 'MOZAMBIQUE' and n2.n_name = 'UNITED KINGDOM')
                    or (n1.n_name = 'UNITED KINGDOM' and n2.n_name = 'MOZAMBIQUE')
                )
                and l_shipdate between cast('1995-01-01'as date) and cast('1996-12-31'as date)
        ) as shipping
    group by
        supp_nation,
        cust_nation,
        l_year
    order by
        supp_nation,
        cust_nation,
        l_year
    ''',
    '''
    q7_10_1
    '''
)

run_benchmark_query(
    '''
    select
        supp_nation,
        cust_nation,
        l_year,
        sum(volume) as revenue
    from
        (
            select
                n1.n_name as supp_nation,
                n2.n_name as cust_nation,
                year(l_shipdate) as l_year,
                l_extendedprice * (1 - l_discount) as volume
            from
                supplier,
                lineitem,
                orders,
                customer,
                nation n1,
                nation n2
            where
                s_suppkey = l_suppkey
                and o_orderkey = l_orderkey
                and c_custkey = o_custkey
                and s_nationkey = n1.n_nationkey
                and c_nationkey = n2.n_nationkey
                and (
                    (n1.n_name = 'INDIA' and n2.n_name = 'VIETNAM')
                    or (n1.n_name = 'VIETNAM' and n2.n_name = 'INDIA')
                )
                and l_shipdate between cast('1995-01-01'as date) and cast('1996-12-31'as date)
        ) as shipping
    group by
        supp_nation,
        cust_nation,
        l_year
    order by
        supp_nation,
        cust_nation,
        l_year
    ''',
    '''
    q7_10_2
    '''
)

run_benchmark_query(
    '''
    select
        supp_nation,
        cust_nation,
        l_year,
        sum(volume) as revenue
    from
        (
            select
                n1.n_name as supp_nation,
                n2.n_name as cust_nation,
                year(l_shipdate) as l_year,
                l_extendedprice * (1 - l_discount) as volume
            from
                supplier,
                lineitem,
                orders,
                customer,
                nation n1,
                nation n2
            where
                s_suppkey = l_suppkey
                and o_orderkey = l_orderkey
                and c_custkey = o_custkey
                and s_nationkey = n1.n_nationkey
                and c_nationkey = n2.n_nationkey
                and (
                    (n1.n_name = 'ALGERIA' and n2.n_name = 'SAUDI ARABIA')
                    or (n1.n_name = 'SAUDI ARABIA' and n2.n_name = 'ALGERIA')
                )
                and l_shipdate between cast('1995-01-01'as date) and cast('1996-12-31'as date)
        ) as shipping
    group by
        supp_nation,
        cust_nation,
        l_year
    order by
        supp_nation,
        cust_nation,
        l_year
    ''',
    '''
    q7_10_3
    '''
)

run_benchmark_query(
    '''
    select
        supp_nation,
        cust_nation,
        l_year,
        sum(volume) as revenue
    from
        (
            select
                n1.n_name as supp_nation,
                n2.n_name as cust_nation,
                year(l_shipdate) as l_year,
                l_extendedprice * (1 - l_discount) as volume
            from
                supplier,
                lineitem,
                orders,
                customer,
                nation n1,
                nation n2
            where
                s_suppkey = l_suppkey
                and o_orderkey = l_orderkey
                and c_custkey = o_custkey
                and s_nationkey = n1.n_nationkey
                and c_nationkey = n2.n_nationkey
                and (
                    (n1.n_name = 'PERU' and n2.n_name = 'CHINA')
                    or (n1.n_name = 'CHINA' and n2.n_name = 'PERU')
                )
                and l_shipdate between cast('1995-01-01'as date) and cast('1996-12-31'as date)
        ) as shipping
    group by
        supp_nation,
        cust_nation,
        l_year
    order by
        supp_nation,
        cust_nation,
        l_year
    ''',
    '''
    q7_10_4
    '''
)

run_benchmark_query(
    '''
    select
        supp_nation,
        cust_nation,
        l_year,
        sum(volume) as revenue
    from
        (
            select
                n1.n_name as supp_nation,
                n2.n_name as cust_nation,
                year(l_shipdate) as l_year,
                l_extendedprice * (1 - l_discount) as volume
            from
                supplier,
                lineitem,
                orders,
                customer,
                nation n1,
                nation n2
            where
                s_suppkey = l_suppkey
                and o_orderkey = l_orderkey
                and c_custkey = o_custkey
                and s_nationkey = n1.n_nationkey
                and c_nationkey = n2.n_nationkey
                and (
                    (n1.n_name = 'INDONESIA' and n2.n_name = 'MOZAMBIQUE')
                    or (n1.n_name = 'MOZAMBIQUE' and n2.n_name = 'INDONESIA')
                )
                and l_shipdate between cast('1995-01-01'as date) and cast('1996-12-31'as date)
        ) as shipping
    group by
        supp_nation,
        cust_nation,
        l_year
    order by
        supp_nation,
        cust_nation,
        l_year
    ''',
    '''
    q7_10_5
    '''
)

run_benchmark_query(
    '''
    select
        supp_nation,
        cust_nation,
        l_year,
        sum(volume) as revenue
    from
        (
            select
                n1.n_name as supp_nation,
                n2.n_name as cust_nation,
                year(l_shipdate) as l_year,
                l_extendedprice * (1 - l_discount) as volume
            from
                supplier,
                lineitem,
                orders,
                customer,
                nation n1,
                nation n2
            where
                s_suppkey = l_suppkey
                and o_orderkey = l_orderkey
                and c_custkey = o_custkey
                and s_nationkey = n1.n_nationkey
                and c_nationkey = n2.n_nationkey
                and (
                    (n1.n_name = 'ARGENTINA' and n2.n_name = 'MOROCCO')
                    or (n1.n_name = 'MOROCCO' and n2.n_name = 'ARGENTINA')
                )
                and l_shipdate between cast('1995-01-01'as date) and cast('1996-12-31'as date)
        ) as shipping
    group by
        supp_nation,
        cust_nation,
        l_year
    order by
        supp_nation,
        cust_nation,
        l_year
    ''',
    '''
    q7_10_6
    '''
)

run_benchmark_query(
    '''
    select
        supp_nation,
        cust_nation,
        l_year,
        sum(volume) as revenue
    from
        (
            select
                n1.n_name as supp_nation,
                n2.n_name as cust_nation,
                year(l_shipdate) as l_year,
                l_extendedprice * (1 - l_discount) as volume
            from
                supplier,
                lineitem,
                orders,
                customer,
                nation n1,
                nation n2
            where
                s_suppkey = l_suppkey
                and o_orderkey = l_orderkey
                and c_custkey = o_custkey
                and s_nationkey = n1.n_nationkey
                and c_nationkey = n2.n_nationkey
                and (
                    (n1.n_name = 'CHINA' and n2.n_name = 'JORDAN')
                    or (n1.n_name = 'JORDAN' and n2.n_name = 'CHINA')
                )
                and l_shipdate between cast('1995-01-01'as date) and cast('1996-12-31'as date)
        ) as shipping
    group by
        supp_nation,
        cust_nation,
        l_year
    order by
        supp_nation,
        cust_nation,
        l_year
    ''',
    '''
    q7_10_7
    '''
)

run_benchmark_query(
    '''
    select
        supp_nation,
        cust_nation,
        l_year,
        sum(volume) as revenue
    from
        (
            select
                n1.n_name as supp_nation,
                n2.n_name as cust_nation,
                year(l_shipdate) as l_year,
                l_extendedprice * (1 - l_discount) as volume
            from
                supplier,
                lineitem,
                orders,
                customer,
                nation n1,
                nation n2
            where
                s_suppkey = l_suppkey
                and o_orderkey = l_orderkey
                and c_custkey = o_custkey
                and s_nationkey = n1.n_nationkey
                and c_nationkey = n2.n_nationkey
                and (
                    (n1.n_name = 'IRAN' and n2.n_name = 'IRAQ')
                    or (n1.n_name = 'IRAQ' and n2.n_name = 'IRAN')
                )
                and l_shipdate between cast('1995-01-01'as date) and cast('1996-12-31'as date)
        ) as shipping
    group by
        supp_nation,
        cust_nation,
        l_year
    order by
        supp_nation,
        cust_nation,
        l_year
    ''',
    '''
    q7_10_8
    '''
)

run_benchmark_query(
    '''
    select
        supp_nation,
        cust_nation,
        l_year,
        sum(volume) as revenue
    from
        (
            select
                n1.n_name as supp_nation,
                n2.n_name as cust_nation,
                year(l_shipdate) as l_year,
                l_extendedprice * (1 - l_discount) as volume
            from
                supplier,
                lineitem,
                orders,
                customer,
                nation n1,
                nation n2
            where
                s_suppkey = l_suppkey
                and o_orderkey = l_orderkey
                and c_custkey = o_custkey
                and s_nationkey = n1.n_nationkey
                and c_nationkey = n2.n_nationkey
                and (
                    (n1.n_name = 'BRAZIL' and n2.n_name = 'IRAN')
                    or (n1.n_name = 'IRAN' and n2.n_name = 'BRAZIL')
                )
                and l_shipdate between cast('1995-01-01'as date) and cast('1996-12-31'as date)
        ) as shipping
    group by
        supp_nation,
        cust_nation,
        l_year
    order by
        supp_nation,
        cust_nation,
        l_year
    ''',
    '''
    q7_10_9
    '''
)

run_benchmark_query(
    '''
    select
        supp_nation,
        cust_nation,
        l_year,
        sum(volume) as revenue
    from
        (
            select
                n1.n_name as supp_nation,
                n2.n_name as cust_nation,
                year(l_shipdate) as l_year,
                l_extendedprice * (1 - l_discount) as volume
            from
                supplier,
                lineitem,
                orders,
                customer,
                nation n1,
                nation n2
            where
                s_suppkey = l_suppkey
                and o_orderkey = l_orderkey
                and c_custkey = o_custkey
                and s_nationkey = n1.n_nationkey
                and c_nationkey = n2.n_nationkey
                and (
                    (n1.n_name = 'ROMANIA' and n2.n_name = 'INDIA')
                    or (n1.n_name = 'INDIA' and n2.n_name = 'ROMANIA')
                )
                and l_shipdate between cast('1995-01-01'as date) and cast('1996-12-31'as date)
        ) as shipping
    group by
        supp_nation,
        cust_nation,
        l_year
    order by
        supp_nation,
        cust_nation,
        l_year
    ''',
    '''
    q7_10_10
    '''
)

run_benchmark_query(
    '''
    select
        c_custkey,
        c_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        c_acctbal,
        n_name,
        c_address,
        c_phone,
        c_comment
    from
        customer,
        orders,
        lineitem,
        nation
    where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate >= cast('1993-02-01'as date)
        and o_orderdate < add_months(cast('1993-02-01'as date), '3')
        and l_returnflag = 'R'
        and c_nationkey = n_nationkey
    group by
        c_custkey,
        c_name,
        c_acctbal,
        c_phone,
        n_name,
        c_address,
        c_comment
    order by
        revenue desc
    limit 20
    ''',
    '''
    q10_10_1
    '''
)

run_benchmark_query(
    '''
    select
        c_custkey,
        c_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        c_acctbal,
        n_name,
        c_address,
        c_phone,
        c_comment
    from
        customer,
        orders,
        lineitem,
        nation
    where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate >= cast('1993-11-01'as date)
        and o_orderdate < add_months(cast('1993-11-01'as date), '3')
        and l_returnflag = 'R'
        and c_nationkey = n_nationkey
    group by
        c_custkey,
        c_name,
        c_acctbal,
        c_phone,
        n_name,
        c_address,
        c_comment
    order by
        revenue desc
    limit 20
    ''',
    '''
    q10_10_2
    '''
)

run_benchmark_query(
    '''
    select
        c_custkey,
        c_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        c_acctbal,
        n_name,
        c_address,
        c_phone,
        c_comment
    from
        customer,
        orders,
        lineitem,
        nation
    where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate >= cast('1994-08-01'as date)
        and o_orderdate < add_months(cast('1994-08-01'as date), '3')
        and l_returnflag = 'R'
        and c_nationkey = n_nationkey
    group by
        c_custkey,
        c_name,
        c_acctbal,
        c_phone,
        n_name,
        c_address,
        c_comment
    order by
        revenue desc
    limit 20
    ''',
    '''
    q10_10_3
    '''
)

run_benchmark_query(
    '''
    select
        c_custkey,
        c_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        c_acctbal,
        n_name,
        c_address,
        c_phone,
        c_comment
    from
        customer,
        orders,
        lineitem,
        nation
    where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate >= cast('1993-05-01'as date)
        and o_orderdate < add_months(cast('1993-05-01'as date), '3')
        and l_returnflag = 'R'
        and c_nationkey = n_nationkey
    group by
        c_custkey,
        c_name,
        c_acctbal,
        c_phone,
        n_name,
        c_address,
        c_comment
    order by
        revenue desc
    limit 20
    ''',
    '''
    q10_10_4
    '''
)

run_benchmark_query(
    '''
    select
        c_custkey,
        c_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        c_acctbal,
        n_name,
        c_address,
        c_phone,
        c_comment
    from
        customer,
        orders,
        lineitem,
        nation
    where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate >= cast('1994-02-01'as date)
        and o_orderdate < add_months(cast('1994-02-01'as date), '3')
        and l_returnflag = 'R'
        and c_nationkey = n_nationkey
    group by
        c_custkey,
        c_name,
        c_acctbal,
        c_phone,
        n_name,
        c_address,
        c_comment
    order by
        revenue desc
    limit 20
    ''',
    '''
    q10_10_5
    '''
)

run_benchmark_query(
    '''
    select
        c_custkey,
        c_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        c_acctbal,
        n_name,
        c_address,
        c_phone,
        c_comment
    from
        customer,
        orders,
        lineitem,
        nation
    where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate >= cast('1994-12-01'as date)
        and o_orderdate < add_months(cast('1994-12-01'as date), '3')
        and l_returnflag = 'R'
        and c_nationkey = n_nationkey
    group by
        c_custkey,
        c_name,
        c_acctbal,
        c_phone,
        n_name,
        c_address,
        c_comment
    order by
        revenue desc
    limit 20
    ''',
    '''
    q10_10_6
    '''
)

run_benchmark_query(
    '''
    select
        c_custkey,
        c_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        c_acctbal,
        n_name,
        c_address,
        c_phone,
        c_comment
    from
        customer,
        orders,
        lineitem,
        nation
    where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate >= cast('1993-09-01'as date)
        and o_orderdate < add_months(cast('1993-09-01'as date), '3')
        and l_returnflag = 'R'
        and c_nationkey = n_nationkey
    group by
        c_custkey,
        c_name,
        c_acctbal,
        c_phone,
        n_name,
        c_address,
        c_comment
    order by
        revenue desc
    limit 20
    ''',
    '''
    q10_10_7
    '''
)

run_benchmark_query(
    '''
    select
        c_custkey,
        c_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        c_acctbal,
        n_name,
        c_address,
        c_phone,
        c_comment
    from
        customer,
        orders,
        lineitem,
        nation
    where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate >= cast('1994-06-01'as date)
        and o_orderdate < add_months(cast('1994-06-01'as date), '3')
        and l_returnflag = 'R'
        and c_nationkey = n_nationkey
    group by
        c_custkey,
        c_name,
        c_acctbal,
        c_phone,
        n_name,
        c_address,
        c_comment
    order by
        revenue desc
    limit 20
    ''',
    '''
    q10_10_8
    '''
)

run_benchmark_query(
    '''
    select
        c_custkey,
        c_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        c_acctbal,
        n_name,
        c_address,
        c_phone,
        c_comment
    from
        customer,
        orders,
        lineitem,
        nation
    where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate >= cast('1993-03-01'as date)
        and o_orderdate < add_months(cast('1993-03-01'as date), '3')
        and l_returnflag = 'R'
        and c_nationkey = n_nationkey
    group by
        c_custkey,
        c_name,
        c_acctbal,
        c_phone,
        n_name,
        c_address,
        c_comment
    order by
        revenue desc
    limit 20
    ''',
    '''
    q10_10_9
    '''
)

run_benchmark_query(
    '''
    select
        c_custkey,
        c_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        c_acctbal,
        n_name,
        c_address,
        c_phone,
        c_comment
    from
        customer,
        orders,
        lineitem,
        nation
    where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate >= cast('1993-12-01'as date)
        and o_orderdate < add_months(cast('1993-12-01'as date), '3')
        and l_returnflag = 'R'
        and c_nationkey = n_nationkey
    group by
        c_custkey,
        c_name,
        c_acctbal,
        c_phone,
        n_name,
        c_address,
        c_comment
    order by
        revenue desc
    limit 20
    ''',
    '''
    q10_10_10
    '''
)

run_benchmark_query(
    '''
    select
        nation,
        o_year,
        sum(amount) as sum_profit
    from
        (
            select
                n_name as nation,
                year(o_orderdate) as o_year,
                l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
            from
                part,
                lineitem,
                supplier,
                partsupp,
                orders,
                nation
            where
                s_suppkey = l_suppkey
                and ps_suppkey = l_suppkey
                and ps_partkey = l_partkey
                and p_partkey = l_partkey
                and o_orderkey = l_orderkey
                and s_nationkey = n_nationkey
                and p_name like '%almond%'
        ) as profit
    group by
        nation,
        o_year
    order by
        nation,
        o_year desc
    ''',
    '''
    q9_10_1
    '''
)

run_benchmark_query(
    '''
    select
        nation,
        o_year,
        sum(amount) as sum_profit
    from
        (
            select
                n_name as nation,
                year(o_orderdate) as o_year,
                l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
            from
                part,
                lineitem,
                supplier,
                partsupp,
                orders,
                nation
            where
                s_suppkey = l_suppkey
                and ps_suppkey = l_suppkey
                and ps_partkey = l_partkey
                and p_partkey = l_partkey
                and o_orderkey = l_orderkey
                and s_nationkey = n_nationkey
                and p_name like '%thistle%'
        ) as profit
    group by
        nation,
        o_year
    order by
        nation,
        o_year desc
    ''',
    '''
    q9_10_2
    '''
)

run_benchmark_query(
    '''
    select
        nation,
        o_year,
        sum(amount) as sum_profit
    from
        (
            select
                n_name as nation,
                year(o_orderdate) as o_year,
                l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
            from
                part,
                lineitem,
                supplier,
                partsupp,
                orders,
                nation
            where
                s_suppkey = l_suppkey
                and ps_suppkey = l_suppkey
                and ps_partkey = l_partkey
                and p_partkey = l_partkey
                and o_orderkey = l_orderkey
                and s_nationkey = n_nationkey
                and p_name like '%slate%'
        ) as profit
    group by
        nation,
        o_year
    order by
        nation,
        o_year desc
    ''',
    '''
    q9_10_3
    '''
)

run_benchmark_query(
    '''
    select
        nation,
        o_year,
        sum(amount) as sum_profit
    from
        (
            select
                n_name as nation,
                year(o_orderdate) as o_year,
                l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
            from
                part,
                lineitem,
                supplier,
                partsupp,
                orders,
                nation
            where
                s_suppkey = l_suppkey
                and ps_suppkey = l_suppkey
                and ps_partkey = l_partkey
                and p_partkey = l_partkey
                and o_orderkey = l_orderkey
                and s_nationkey = n_nationkey
                and p_name like '%saddle%'
        ) as profit
    group by
        nation,
        o_year
    order by
        nation,
        o_year desc
    ''',
    '''
    q9_10_4
    '''
)

run_benchmark_query(
    '''
    select
        nation,
        o_year,
        sum(amount) as sum_profit
    from
        (
            select
                n_name as nation,
                year(o_orderdate) as o_year,
                l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
            from
                part,
                lineitem,
                supplier,
                partsupp,
                orders,
                nation
            where
                s_suppkey = l_suppkey
                and ps_suppkey = l_suppkey
                and ps_partkey = l_partkey
                and p_partkey = l_partkey
                and o_orderkey = l_orderkey
                and s_nationkey = n_nationkey
                and p_name like '%puff%'
        ) as profit
    group by
        nation,
        o_year
    order by
        nation,
        o_year desc
    ''',
    '''
    q9_10_5
    '''
)

run_benchmark_query(
    '''
    select
        nation,
        o_year,
        sum(amount) as sum_profit
    from
        (
            select
                n_name as nation,
                year(o_orderdate) as o_year,
                l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
            from
                part,
                lineitem,
                supplier,
                partsupp,
                orders,
                nation
            where
                s_suppkey = l_suppkey
                and ps_suppkey = l_suppkey
                and ps_partkey = l_partkey
                and p_partkey = l_partkey
                and o_orderkey = l_orderkey
                and s_nationkey = n_nationkey
                and p_name like '%papaya%'
        ) as profit
    group by
        nation,
        o_year
    order by
        nation,
        o_year desc
    ''',
    '''
    q9_10_6
    '''
)

run_benchmark_query(
    '''
    select
        nation,
        o_year,
        sum(amount) as sum_profit
    from
        (
            select
                n_name as nation,
                year(o_orderdate) as o_year,
                l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
            from
                part,
                lineitem,
                supplier,
                partsupp,
                orders,
                nation
            where
                s_suppkey = l_suppkey
                and ps_suppkey = l_suppkey
                and ps_partkey = l_partkey
                and p_partkey = l_partkey
                and o_orderkey = l_orderkey
                and s_nationkey = n_nationkey
                and p_name like '%navajo%'
        ) as profit
    group by
        nation,
        o_year
    order by
        nation,
        o_year desc
    ''',
    '''
    q9_10_7
    '''
)

run_benchmark_query(
    '''
    select
        nation,
        o_year,
        sum(amount) as sum_profit
    from
        (
            select
                n_name as nation,
                year(o_orderdate) as o_year,
                l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
            from
                part,
                lineitem,
                supplier,
                partsupp,
                orders,
                nation
            where
                s_suppkey = l_suppkey
                and ps_suppkey = l_suppkey
                and ps_partkey = l_partkey
                and p_partkey = l_partkey
                and o_orderkey = l_orderkey
                and s_nationkey = n_nationkey
                and p_name like '%medium%'
        ) as profit
    group by
        nation,
        o_year
    order by
        nation,
        o_year desc
    ''',
    '''
    q9_10_8
    '''
)

run_benchmark_query(
    '''
    select
        nation,
        o_year,
        sum(amount) as sum_profit
    from
        (
            select
                n_name as nation,
                year(o_orderdate) as o_year,
                l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
            from
                part,
                lineitem,
                supplier,
                partsupp,
                orders,
                nation
            where
                s_suppkey = l_suppkey
                and ps_suppkey = l_suppkey
                and ps_partkey = l_partkey
                and p_partkey = l_partkey
                and o_orderkey = l_orderkey
                and s_nationkey = n_nationkey
                and p_name like '%lemon%'
        ) as profit
    group by
        nation,
        o_year
    order by
        nation,
        o_year desc
    ''',
    '''
    q9_10_9
    '''
)

run_benchmark_query(
    '''
    select
        nation,
        o_year,
        sum(amount) as sum_profit
    from
        (
            select
                n_name as nation,
                year(o_orderdate) as o_year,
                l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
            from
                part,
                lineitem,
                supplier,
                partsupp,
                orders,
                nation
            where
                s_suppkey = l_suppkey
                and ps_suppkey = l_suppkey
                and ps_partkey = l_partkey
                and p_partkey = l_partkey
                and o_orderkey = l_orderkey
                and s_nationkey = n_nationkey
                and p_name like '%indian%'
        ) as profit
    group by
        nation,
        o_year
    order by
        nation,
        o_year desc
    ''',
    '''
    q9_10_10
    '''
)

run_benchmark_query(
    '''
    select
        sum(l_extendedprice* (1 - l_discount)) as revenue
    from
        lineitem,
        part
    where
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#11'
            and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
            and l_quantity >= 1 and l_quantity <= 1 + 10
            and p_size between 1 and 5
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#11'
            and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
            and l_quantity >= 10 and l_quantity <= 10 + 10
            and p_size between 1 and 10
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#11'
            and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
            and l_quantity >= 20 and l_quantity <= 20 + 10
            and p_size between 1 and 15
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
    ''',
    '''
    q19_10_1
    '''
)

run_benchmark_query(
    '''
    select
        sum(l_extendedprice* (1 - l_discount)) as revenue
    from
        lineitem,
        part
    where
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#13'
            and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
            and l_quantity >= 6 and l_quantity <= 6 + 10
            and p_size between 1 and 5
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#43'
            and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
            and l_quantity >= 11 and l_quantity <= 11 + 10
            and p_size between 1 and 10
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#55'
            and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
            and l_quantity >= 27 and l_quantity <= 27 + 10
            and p_size between 1 and 15
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
    ''',
    '''
    q19_10_2
    '''
)

run_benchmark_query(
    '''
    select
        sum(l_extendedprice* (1 - l_discount)) as revenue
    from
        lineitem,
        part
    where
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#15'
            and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
            and l_quantity >= 1 and l_quantity <= 1 + 10
            and p_size between 1 and 5
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#21'
            and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
            and l_quantity >= 12 and l_quantity <= 12 + 10
            and p_size between 1 and 10
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#54'
            and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
            and l_quantity >= 23 and l_quantity <= 23 + 10
            and p_size between 1 and 15
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
    ''',
    '''
    q19_10_3
    '''
)

run_benchmark_query(
    '''
    select
        sum(l_extendedprice* (1 - l_discount)) as revenue
    from
        lineitem,
        part
    where
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#22'
            and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
            and l_quantity >= 6 and l_quantity <= 6 + 10
            and p_size between 1 and 5
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#14'
            and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
            and l_quantity >= 13 and l_quantity <= 13 + 10
            and p_size between 1 and 10
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#43'
            and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
            and l_quantity >= 30 and l_quantity <= 30 + 10
            and p_size between 1 and 15
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
    ''',
    '''
    q19_10_4
    '''
)

run_benchmark_query(
    '''
    select
        sum(l_extendedprice* (1 - l_discount)) as revenue
    from
        lineitem,
        part
    where
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#24'
            and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
            and l_quantity >= 2 and l_quantity <= 2 + 10
            and p_size between 1 and 5
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#42'
            and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
            and l_quantity >= 14 and l_quantity <= 14 + 10
            and p_size between 1 and 10
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#42'
            and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
            and l_quantity >= 26 and l_quantity <= 26 + 10
            and p_size between 1 and 15
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
    ''',
    '''
    q19_10_5
    '''
)

run_benchmark_query(
    '''
    select
        sum(l_extendedprice* (1 - l_discount)) as revenue
    from
        lineitem,
        part
    where
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#21'
            and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
            and l_quantity >= 7 and l_quantity <= 7 + 10
            and p_size between 1 and 5
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#35'
            and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
            and l_quantity >= 15 and l_quantity <= 15 + 10
            and p_size between 1 and 10
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#42'
            and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
            and l_quantity >= 22 and l_quantity <= 22 + 10
            and p_size between 1 and 15
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
    ''',
    '''
    q19_10_6
    '''
)

run_benchmark_query(
    '''
    select
        sum(l_extendedprice* (1 - l_discount)) as revenue
    from
        lineitem,
        part
    where
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#33'
            and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
            and l_quantity >= 2 and l_quantity <= 2 + 10
            and p_size between 1 and 5
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#13'
            and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
            and l_quantity >= 16 and l_quantity <= 16 + 10
            and p_size between 1 and 10
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#31'
            and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
            and l_quantity >= 30 and l_quantity <= 30 + 10
            and p_size between 1 and 15
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
    ''',
    '''
    q19_10_7
    '''
)

run_benchmark_query(
    '''
    select
        sum(l_extendedprice* (1 - l_discount)) as revenue
    from
        lineitem,
        part
    where
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#35'
            and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
            and l_quantity >= 7 and l_quantity <= 7 + 10
            and p_size between 1 and 5
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#51'
            and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
            and l_quantity >= 17 and l_quantity <= 17 + 10
            and p_size between 1 and 10
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#35'
            and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
            and l_quantity >= 26 and l_quantity <= 26 + 10
            and p_size between 1 and 15
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
    ''',
    '''
    q19_10_8
    '''
)

run_benchmark_query(
    '''
    select
        sum(l_extendedprice* (1 - l_discount)) as revenue
    from
        lineitem,
        part
    where
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#32'
            and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
            and l_quantity >= 3 and l_quantity <= 3 + 10
            and p_size between 1 and 5
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#34'
            and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
            and l_quantity >= 18 and l_quantity <= 18 + 10
            and p_size between 1 and 10
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#34'
            and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
            and l_quantity >= 22 and l_quantity <= 22 + 10
            and p_size between 1 and 15
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
    ''',
    '''
    q19_10_9
    '''
)

run_benchmark_query(
    '''
    select
        sum(l_extendedprice* (1 - l_discount)) as revenue
    from
        lineitem,
        part
    where
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#44'
            and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
            and l_quantity >= 8 and l_quantity <= 8 + 10
            and p_size between 1 and 5
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#12'
            and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
            and l_quantity >= 19 and l_quantity <= 19 + 10
            and p_size between 1 and 10
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#24'
            and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
            and l_quantity >= 29 and l_quantity <= 29 + 10
            and p_size between 1 and 15
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
    ''',
    '''
    q19_10_10
    '''
)

run_benchmark_query(
    '''
    select
        c_count,
        count(*) as custdist
    from
        (
            select
                c_custkey,
                count(o_orderkey) c_count
            from
                customer left outer join orders on
                    c_custkey = o_custkey
                    and o_comment not like '%special%packages%'
            group by
                c_custkey
        ) as c_orders
    group by
        c_count
    order by
        custdist desc,
        c_count desc
    ''',
    '''
    q13_10_1
    '''
)

run_benchmark_query(
    '''
    select
        c_count,
        count(*) as custdist
    from
        (
            select
                c_custkey,
                count(o_orderkey) c_count
            from
                customer left outer join orders on
                    c_custkey = o_custkey
                    and o_comment not like '%pending%accounts%'
            group by
                c_custkey
        ) as c_orders
    group by
        c_count
    order by
        custdist desc,
        c_count desc
    ''',
    '''
    q13_10_2
    '''
)

run_benchmark_query(
    '''
    select
        c_count,
        count(*) as custdist
    from
        (
            select
                c_custkey,
                count(o_orderkey) c_count
            from
                customer left outer join orders on
                    c_custkey = o_custkey
                    and o_comment not like '%express%packages%'
            group by
                c_custkey
        ) as c_orders
    group by
        c_count
    order by
        custdist desc,
        c_count desc
    ''',
    '''
    q13_10_3
    '''
)

run_benchmark_query(
    '''
    select
        c_count,
        count(*) as custdist
    from
        (
            select
                c_custkey,
                count(o_orderkey) c_count
            from
                customer left outer join orders on
                    c_custkey = o_custkey
                    and o_comment not like '%special%deposits%'
            group by
                c_custkey
        ) as c_orders
    group by
        c_count
    order by
        custdist desc,
        c_count desc
    ''',
    '''
    q13_10_4
    '''
)

run_benchmark_query(
    '''
    select
        c_count,
        count(*) as custdist
    from
        (
            select
                c_custkey,
                count(o_orderkey) c_count
            from
                customer left outer join orders on
                    c_custkey = o_custkey
                    and o_comment not like '%unusual%requests%'
            group by
                c_custkey
        ) as c_orders
    group by
        c_count
    order by
        custdist desc,
        c_count desc
    ''',
    '''
    q13_10_5
    '''
)

run_benchmark_query(
    '''
    select
        c_count,
        count(*) as custdist
    from
        (
            select
                c_custkey,
                count(o_orderkey) c_count
            from
                customer left outer join orders on
                    c_custkey = o_custkey
                    and o_comment not like '%express%deposits%'
            group by
                c_custkey
        ) as c_orders
    group by
        c_count
    order by
        custdist desc,
        c_count desc
    ''',
    '''
    q13_10_6
    '''
)

run_benchmark_query(
    '''
    select
        c_count,
        count(*) as custdist
    from
        (
            select
                c_custkey,
                count(o_orderkey) c_count
            from
                customer left outer join orders on
                    c_custkey = o_custkey
                    and o_comment not like '%pending%accounts%'
            group by
                c_custkey
        ) as c_orders
    group by
        c_count
    order by
        custdist desc,
        c_count desc
    ''',
    '''
    q13_10_7
    '''
)

run_benchmark_query(
    '''
    select
        c_count,
        count(*) as custdist
    from
        (
            select
                c_custkey,
                count(o_orderkey) c_count
            from
                customer left outer join orders on
                    c_custkey = o_custkey
                    and o_comment not like '%unusual%packages%'
            group by
                c_custkey
        ) as c_orders
    group by
        c_count
    order by
        custdist desc,
        c_count desc
    ''',
    '''
    q13_10_8
    '''
)

run_benchmark_query(
    '''
    select
        c_count,
        count(*) as custdist
    from
        (
            select
                c_custkey,
                count(o_orderkey) c_count
            from
                customer left outer join orders on
                    c_custkey = o_custkey
                    and o_comment not like '%special%accounts%'
            group by
                c_custkey
        ) as c_orders
    group by
        c_count
    order by
        custdist desc,
        c_count desc
    ''',
    '''
    q13_10_9
    '''
)

run_benchmark_query(
    '''
    select
        c_count,
        count(*) as custdist
    from
        (
            select
                c_custkey,
                count(o_orderkey) c_count
            from
                customer left outer join orders on
                    c_custkey = o_custkey
                    and o_comment not like '%pending%requests%'
            group by
                c_custkey
        ) as c_orders
    group by
        c_count
    order by
        custdist desc,
        c_count desc
    ''',
    '''
    q13_10_10
    '''
)

url = "http://aqa-demo-scratchpad.s3-website-us-west-2.amazonaws.com/googlecharter.html?data="
url += "[" + ",".join(str(run) for run in runtimes) + "]"
print("""-------- Query Finished. --------
-------- Benchmark Results Viewer: {} --------""".format(url))

bench_or_base = "benchmark"
level = "L1"
data_size = "1M"

if not os.path.exists("./results/"+bench_or_base+"/"):
    os.makedirs("./results/"+bench_or_base+"/")

if not os.path.exists("./results/"+bench_or_base+"/"+level+"_"+data_size+".csv"):
    with open("./results/"+bench_or_base+"/"+level+"_"+data_size+".csv", "w") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["Timestamp"] + list(range(1, len(runtimes)+1)))
        csv_file.close()

with open("./results/"+bench_or_base+"/"+level+"_"+data_size+".csv", "a") as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow([datetime.now()] + runtimes)
    csv_file.close()
