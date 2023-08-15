from pyspark.sql import SparkSession
import findspark
from pyspark.sql import functions as F
from select import select

findspark.add_packages('org.postgresql:postgresql:42.2.22')
from pyspark.sql.functions import min, max, sum, count, col, abs, when, concat, avg, round, dense_rank
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

import sys

if __name__ == '__main__':
    # Configure SparkSession
    spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
    # tableName = sys.argv[0]

    delivery_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='delivery_1173',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    delivery_df.printSchema()
    delivery_df.show()


    # SQL QUERY :
    # select
    #     round(
    #         sum(
    #             case
    #                 when(order_date=customer_pref_delivery_date) then 1
    #                 else 0
    #             end)*100 / count(delivery_id), 2) as immediate_percentage
    # from delivery

    immediate_percentage_df = delivery_df.select(
        round(
            (sum(when(col("order_date") == col("customer_pref_delivery_date"), 1).otherwise(0)) * 100) /
            count("delivery_id"),
            2
        ).alias("immediate_percentage")
    )
    immediate_percentage_df.show()

    # delivery_df = delivery_df.withColumn("immediate_percentage",
    #             when(delivery_df["order_date"] == delivery_df["customer_pref_delivery_date"],1).
    #             otherwise(0))
    #
    # sum_data_df = delivery_df.where(col("order_date") == col("customer_pref_delivery_date")).\
    #               agg(sum(delivery_df["immediate_percentage"]))
    #
    # total_count = delivery_df.select("delivery_id").count()

    # result_data = round(((sum_data)*100/total_count),2)



    # delivery_df.show()
    # print("sum_data",sum_data)
    # print("total_count",total_count)
    # print("result_data", result_data)









