from pyspark.sql import SparkSession
import findspark
from pyspark.sql import functions as F

from pyspark.sql.functions import countDistinct

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
        dbtable='views_1149',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    views_df.printSchema()
    views_df.show()


    # SQL QUERY :
    # select
    #     distinct(viewer_id) as id
    # from views group by viewer_id, view_date
    # having count(distinct(article_id)) >= 2

    article_df = views_df\
        .groupBy("article_id")\
        .agg(
            count("article_id").alias("tot_count")
        ).distinct()
    # viewer_df = views_df.groupBy("viewer_id","view_date").count()
    viewer_df = views_df.groupBy("viewer_id", "view_date").agg(countDistinct("article_id").alias("tot_count"))

    article_df.show()
    viewer_df.show()
    result_df = viewer_df\
        .where(viewer_df["tot_count"] >= 2)\
        .select(col("viewer_id").alias("id")).distinct()
    result_df.show()
















