from pyspark.sql import SparkSession
import findspark
from pyspark.sql import functions as F

from pyspark.sql.functions import countDistinct, expr, month, concat_ws, collect_list, collect_set, sort_array

findspark.add_packages('org.postgresql:postgresql:42.2.22')
from pyspark.sql.functions import min, max, sum, count, col, abs, when, concat, avg, round, dense_rank
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

import sys


if __name__ == '__main__':
    # Configure SparkSession
    spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
    # tableName = sys.argv[0]

    activities_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='activities_1484',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    activities_df.printSchema()
    activities_df.show()

    #sql
    # select
    #     sell_date,
    #     count(distinct(product)) as num_sold,
    #     group_concat(distinct product order by product SEPARATOR ',' ) as products
    # from activities
    # group by sell_date

    final_df = activities_df.groupBy("sell_date").\
        agg(
            countDistinct("product").alias("num_sold"),
            sort_array(collect_set("product")).alias("products")
            # concat_ws(",", collect_list("product")).alias("products")
            # concat("product").alias("products")
            # concat(countDistinct("product")).alias("products")
            # concat_ws(",", countDistinct("product")).alias("products")
        )\
        .orderBy(col("num_sold").desc())

    final_df.show()


























