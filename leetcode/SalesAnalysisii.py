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
    #         product.product_id,
    #         product_name,
    #         buyer_id
    #     from product join sales on product.product_id = sales.product_id
    # )
    #     select
    #         distinct(buyer_id)
    #     from cte where product_name = 'S8' and buyer_id not in (select buyer_id from cte where product_name ='iPhone')


    joined_df = product_df.join(sales_df,product_df["product_id"] == sales_df["product_id"]).\
        select("buyer_id",product_df["product_id"],"product_name")

    # distinct_values_list = ordf.select("sales_id").distinct().rdd.map(lambda row: row[0]).collect()
    # print("distinct_values_list", distinct_values_list)
    print("*********************")
    joined_df.show()
    buyer_list = joined_df.select("buyer_id").where(joined_df["product_name"] == 'iPhone').rdd.map(lambda row: row[0]).collect()
    print("buyer_list",buyer_list)
    joined_df = joined_df.select("buyer_id").distinct().where((col("product_name") == "S8") & (~col("buyer_id").isin(buyer_list)))

    # where(col("product_name") == "S8" , col("buyer_id") != buyer_list)
    print("#################")
    joined_df.show()







