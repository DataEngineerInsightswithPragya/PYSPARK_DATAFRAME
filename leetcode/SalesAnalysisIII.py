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
    # select
    #     s.product_id,
    #     p.product_name
    # from product p join sales s on p.product_id = s.product_id group by s.product_id
    # having min(s.sale_date) >= "2019-01-01" and max(s.sale_date) <= "2019-03-31"

    joined_df = sales_df.join(product_df , sales_df["product_id"] == product_df["product_id"]).\
    select(sales_df["product_id"],product_df["product_name"],"sale_date")

    grouped_df = joined_df.groupBy("product_id").agg(
        min("sale_date").alias("min_sale_date"),
        max("sale_date").alias("max_sale_date")
    )

    filtered_df = grouped_df.filter(
        (col("min_sale_date") >= "2019-01-01") & (col("max_sale_date") <= "2019-03-31")
    )

    grouped_df.show()
    print("filtered_df")
    filtered_df.show()


    joined_df.show()

    result_df = filtered_df.join(product_df, "product_id").select(
        col("product_id"),
        col("product_name")
    )

    result_df.show()












