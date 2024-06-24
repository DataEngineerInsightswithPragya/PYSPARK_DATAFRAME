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
    # with cte as (select
    #         c.customer_id,
    #         c.name,
    #         price,
    #         quantity,
    #         month(order_date) as month
    # from customers c join orders o on c.customer_id = o.customer_id
    # join product p on p.product_id = o.product_id
    # where order_date >= '2020-06-01' and order_date <= '2020-07-31')
    # select
    #     customer_id,
    #     name
    # from cte
    # group by customer_id
    # having sum(case when month = 6 then price * quantity end) >= 100
    # and sum(case when month = 7 then price * quantity end) >= 100

    joined_df = customer_df.join(orders_df,customer_df["customer_id"] == orders_df["customer_id"])\
        .join(product_df,product_df["product_id"] == orders_df["product_id"])\
        .where((orders_df["order_date"] >= '2020-06-01') & (orders_df["order_date"] <= '2020-07-31'))\
        .select(customer_df["customer_id"],customer_df["name"],product_df["price"],
                orders_df["quantity"],month(orders_df["order_date"]).alias("month"))

    joined_df.show()

    final_df = joined_df.groupby(joined_df["customer_id"],joined_df["name"])\
        .agg(
            sum(when(joined_df["month"] == 6 , joined_df["price"] * joined_df["quantity"])).alias("june_month"),
            sum(when(joined_df["month"] == 7, joined_df["price"] * joined_df["quantity"])).alias("july_month"),
    )
    final_df.show()

    final_result_df = final_df.select(final_df["customer_id"],final_df["name"])\
        .where((final_df["june_month"] >= 100) & (final_df["july_month"] >= 100))

    final_result_df.show()

    #spark.stop()

    sys.stdin.readline()
























