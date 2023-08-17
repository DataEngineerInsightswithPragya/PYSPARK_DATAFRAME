from pyspark.sql import SparkSession
import findspark
from pyspark.sql import functions as F

from pyspark.sql.functions import countDistinct, expr, month

findspark.add_packages('org.postgresql:postgresql:42.2.22')
from pyspark.sql.functions import min, max, sum, count, col, abs, when, concat, avg, round, dense_rank
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

import sys


if __name__ == '__main__':
    # Configure SparkSession
    spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
    # tableName = sys.argv[0]

    customer_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='customers_1511',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    customer_df.printSchema()
    customer_df.show()

    product_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='product_1511',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    product_df.printSchema()
    product_df.show()

    orders_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='orders_1511',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    orders_df.printSchema()
    orders_df.show()


    # SQL QUERY :
    # with filter_orders as (
    #     select
    #         *
    #     from orders
    #     where order_date >= '2020-06-01' and order_date <= '2020-07-31'
    # ), joined_order_product as (
    #     select
    #         fo.customer_id,
    #         sum(case
    #         when (order_date >= '2020-06-01' and order_date <= '2020-06-30')  then (price * quantity)
    #         end) as june_total,
    #         sum(case
    #         when (order_date >= '2020-07-01' and order_date <= '2020-07-31')  then (price * quantity)
    #         end) as july_total
    # from filter_orders fo
    # join product p
    # on  fo.product_id = p.product_id
    # group by customer_id
    # )
    # select
    #     c.customer_id,
    #     c.name
    # from joined_order_product j
    # join customers c
    # on j.customer_id = c.customer_id
    # where june_total >= 100 AND july_total >= 100


    filter_order_df = orders_df.select("*").\
        where((orders_df["order_date"] >= '2020-06-01') & (orders_df["order_date"] <= '2020-07-31'))

    joined_order_product_df = filter_order_df.join(product_df,filter_order_df["product_id"] == product_df["product_id"]).\
        groupBy("customer_id").\
        agg(
            sum(when((filter_order_df["order_date"] >= '2020-06-01') & (filter_order_df["order_date"] <= '2020-06-30'),
                     (product_df["price"] * filter_order_df["quantity"]))).alias("june_total"),
            sum(when((filter_order_df["order_date"] >= '2020-07-01') & (filter_order_df["order_date"] <= '2020-07-31'),
                 (product_df["price"] * filter_order_df["quantity"]))).alias("july_total")
        )
    final_df = joined_order_product_df.\
        join(customer_df,joined_order_product_df["customer_id"] == customer_df["customer_id"]).\
        where((joined_order_product_df["june_total"] >= 100) & (joined_order_product_df["july_total"] >= 100)).\
        select(joined_order_product_df["customer_id"],customer_df["name"])

    filter_order_df.show()
    joined_order_product_df.show()
    final_df.show()





















