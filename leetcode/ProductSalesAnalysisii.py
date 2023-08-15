from pyspark.sql import SparkSession
import findspark
from pyspark.sql import functions as F

findspark.add_packages('org.postgresql:postgresql:42.2.22')
from pyspark.sql.functions import min, max, sum, count, col,abs,when,concat
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
        dbtable='product_1068',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    sales_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='sales_1068',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    sales_df.printSchema()
    sales_df.show()

    product_df.printSchema()
    product_df.show()


    # SQL QUERY :
    # select
    #     distinct(product_id),
    #     sum(quantity) over(partition by product_id) as total_quantity
    # from sales;

    windowSpec = Window.partitionBy("product_id")


    # Add a new column "rank_row" that represents the rank of each row within the partition
    sales_df = sales_df.withColumn("total_quantity", sum("quantity").over(windowSpec))
    sales_df = sales_df.select("product_id" , "total_quantity").distinct()
    sales_df.show()





