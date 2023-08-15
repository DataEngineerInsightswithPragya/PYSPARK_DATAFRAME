from pyspark.sql import SparkSession
import findspark
from pyspark.sql import functions as F

findspark.add_packages('org.postgresql:postgresql:42.2.22')
from pyspark.sql.functions import min, max, sum, count, col, abs, when, concat
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
        dbtable='product_2324',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    sales_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='sales_2324',
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
    # WITH intermediate_result_1 AS(
    #     SELECT
    #         product_id,
    #         user_id,
    #         SUM(quantity) AS total
    #     FROM Sales
    #     GROUP BY user_id, product_id
    # ),
    # intermediate_result_2 AS(
    #     SELECT
    #         user_id,
    #         intermediate_result_1.product_id,
    #         RANK() OVER(PARTITION BY user_id ORDER BY total * price DESC) AS rnk
    #     FROM intermediate_result_1 join product on intermediate_result_1.product_id = product.product_id
    # )
    # SELECT
    #     user_id,
    #     product_id
    # FROM intermediate_result_2
    # WHERE rnk = 1

    sales_df = sales_df.groupBy("product_id", "user_id").agg(sum("quantity").alias("total"))
    product_df = product_df.withColumnRenamed("product_id", "productid")
    joined_df = sales_df.join(product_df,sales_df["product_id"] == product_df["productid"])
    windowSpec = Window.partitionBy("user_id").orderBy(col("total") * col("price"))
    joined_df = joined_df.withColumn("total_cost", rank().over(windowSpec))
    joined_df = joined_df.select(joined_df["product_id"],"user_id").where(joined_df["total_cost"] == 1)
    joined_df.show()
