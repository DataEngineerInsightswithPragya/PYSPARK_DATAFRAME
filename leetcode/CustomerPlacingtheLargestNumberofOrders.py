from pyspark.sql import SparkSession
import findspark
findspark.add_packages('org.postgresql:postgresql:42.2.22')
from pyspark.sql.functions import min, max, sum, count
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

import sys


if __name__ == '__main__':
    # Configure SparkSession
    spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
    #tableName = sys.argv[0]

    orders_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='orders_586',
        #dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    orders_df.printSchema()
    orders_df.show()

    #SQL QUERY :
    # with intermediate_result_1 as (
    #     select
    #         customer_number,
    #         count(customer_number) as cust_no
    #     from Orders
    #     group by customer_number
    #  )
    #     select
    #         customer_number
    #     from intermediate_result_1
    #     where cust_no = (select max(cust_no)from intermediate_result_1 )


    orders_df = orders_df.groupBy("customer_number").agg(count("customer_number").alias("cust_no"))
    #orders_df.show()
    max_cust_no = orders_df.agg(max("cust_no")).first()[0]
    print("max_cust_no",max_cust_no)
    #max_cust_no.show()
    order_df = orders_df.select("customer_number").where(orders_df["cust_no"] == max_cust_no)
    #order_df = orders_df.filter(orders_df.cust_no == max_cust_no)
    order_df.show()









