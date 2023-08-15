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
        dbtable='product_2329',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    sales_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='sales_2329',
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
    # with cte as (
    #         select
    #         sales.user_id,
    #         sales.product_id,
    #         sales.quantity,
    #         product.price
    #         from sales join product on sales.product_id = product.product_id
    # ),
    # cte2 as (
    #     select
    #         user_id,
    #         (quantity * price) as spending
    #     from cte
    # )
    #     select
    #         distinct(user_id),
    #         sum(spending) over(partition by user_id) as spending
    #     from cte2
    #     order by spending desc

    sales_product_df = sales_df.join(product_df,sales_df["product_id"] == product_df["product_id"])
    sales_product_df = sales_product_df.withColumn("spending",
                    (sales_product_df["quantity"] * sales_product_df["price"]))
    sales_product_df = sales_product_df.groupBy("user_id").agg(sum("spending").alias("spending"))
    sales_product_df = sales_product_df.select("user_id","spending").orderBy(col("spending").desc())
    # sales_product_df = sales_product_df.select("user",)
    sales_product_df.show()








