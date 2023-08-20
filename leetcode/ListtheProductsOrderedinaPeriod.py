from pyspark.sql import SparkSession
import findspark
from pyspark.sql import functions as F

from pyspark.sql.functions import countDistinct, expr, month, concat_ws, collect_list, collect_set, sort_array

findspark.add_packages('org.postgresql:postgresql:42.2.22')
from pyspark.sql.functions import min, max, sum, count, col, abs, when, concat, avg, round, dense_rank
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

import sys


if __name__ == '__main__':
    # Configure SparkSession
    spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
    # tableName = sys.argv[0]

    products_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='products_1327',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    products_df.printSchema()
    products_df.show()

    orders_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='orders_1327',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    orders_df.printSchema()
    orders_df.show()

    #sql
    # with filter_date as (
    #     select
    #         product_id,
    #         order_date,
    #         unit
    #     from orders
    #     where order_date >= '2020-02-01' and order_date <= '2020-02-29'
    # )
    # select
    #     product_name,
    #     sum(unit) as unit
    # from filter_date
    # join products
    # on filter_date.product_id = products.product_id
    # group by products.product_id having unit >= 100

    filter_order_df = orders_df.select("product_id","order_date","unit").\
        where((col("order_date") >= '2020-02-01') & (col("order_date") <= '2020-02-29'))

    grouped_df = filter_order_df.\
        join(products_df,filter_order_df["product_id"] == products_df["product_id"]).\
        groupBy(products_df["product_id"]).agg(sum("unit").alias("unit"))

    joined_df = grouped_df.\
        join(products_df,grouped_df["product_id"] == products_df["product_id"]).\
        select("product_name",grouped_df["unit"]).where(grouped_df["unit"] >= 100)




    filter_order_df.show()
    grouped_df.show()
    joined_df.show()



























