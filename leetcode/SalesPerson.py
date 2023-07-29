import val as val
from pyspark.sql import SparkSession
import findspark
from pyspark.sql import functions as F

findspark.add_packages('org.postgresql:postgresql:42.2.22')
from pyspark.sql.functions import min, max, sum, count, col,abs
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

import sys

if __name__ == '__main__':
    # Configure SparkSession
    spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
    # tableName = sys.argv[0]

    salesperson_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='salesperson_607',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    company_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='company_607',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    orders_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='orders_607',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()
    salesperson_df.printSchema()
    orders_df.printSchema()
    company_df.printSchema()


    salesperson_df.show()
    orders_df.show()
    company_df.show()

    # SQL QUERY :
    #with intermediate_data as (
    #         select
    #         com_id,
    #         sales_id
    #         from orders
    #         where com_id
    #         in (select com_id from company where name = "RED")
    #     )
    #     select
    #     name
    #     from salesperson where sales_id not in (select distinct (sales_id)
    # from intermediate_data)


    com_id_list = company_df.select("com_id").where(company_df["name"] == "RED").first()[0]
    ordf = orders_df.select("com_id","sales_id").where(orders_df["com_id"] == com_id_list )
    print("com_id_list",com_id_list)
    #df.show()
    distinct_values_list = ordf.select("sales_id").distinct().rdd.map(lambda row: row[0]).collect()
    print("distinct_values_list",distinct_values_list)
    #sales_id_list = df.select("sales_id").distinct().toList
    #print("sales_id_list",sales_id_list)
    df2 = salesperson_df.select("name","sales_id")
    print("****************")
    #df2.show()
    # .where(ordf["sales_id"].isin(distinct_values_list))
    df3 = df2.select("name").where(~col("sales_id").isin(distinct_values_list))
    df3.show()
