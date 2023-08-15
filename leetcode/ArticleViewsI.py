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

    views_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='views_1148',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    views_df.printSchema()
    views_df.show()


    # SQL QUERY :
    # select
    #     distinct(author_id) as id
    # from views where views.author_id = views.viewer_id order by author_id

    views_df = views_df.select(col("author_id").alias("id")).distinct()\
        .where(views_df["author_id"] == views_df["viewer_id"]).orderBy("author_id")
    views_df.show()










