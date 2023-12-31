from pyspark.sql import SparkSession
import findspark
from pyspark.sql import functions as F

findspark.add_packages('org.postgresql:postgresql:42.2.22')
from pyspark.sql.functions import min, max, sum, count, col, abs, when, concat, avg, round, dense_rank
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

import sys

if __name__ == '__main__':
    # Configure SparkSession
    spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
    # tableName = sys.argv[0]

    product_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='product_1082',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    sales_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='sales_1082',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    product_df.printSchema()
    product_df.show()

    sales_df.printSchema()
    sales_df.show()


    # SQL QUERY :
    # with cte as (
    #     select
    #         seller_id,
    #         sum(price) as total_price
    #     from sales
    #     group by seller_id
    # ),
    # cte2 as (
    #     select
    #         seller_id,
    #         dense_rank() over(order by total_price desc) as dense_rnk
    #     from cte
    # )
    # select
    #     seller_id
    # from cte2 where dense_rnk = 1


    sales_df = sales_df.groupBy("seller_id").agg(sum("price").alias("total_price"))
    windowSpec = Window.orderBy(col("total_price").desc())
    sales_df = sales_df.withColumn("dense_rnk",dense_rank().over(windowSpec))
    sales_df = sales_df.select("seller_id").where(col("dense_rnk") == 1)
    sales_df.show()






