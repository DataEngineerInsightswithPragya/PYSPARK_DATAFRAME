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

    npv_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='npv_1421',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    npv_df.printSchema()
    npv_df.show()

    queries_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='queries_1421',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    queries_df.printSchema()
    queries_df.show()

    #sql
    # select
    #     queries.id,
    #     queries.year,
    #     COALESCE(npv, 0) as npv
    #     # CASE WHEN npv IS NULL THEN 0 ELSE npv END AS npv
    #     # IFNULL(npv, 0) as npv
    # from queries left
    # join npv
    # on queries.id = npv.id and queries.year = npv.year

    joined_df = queries_df.\
        join(npv_df,((queries_df["id"] == npv_df["id"]) & (queries_df["year"] == npv_df["year"]) ), "Left").\
        select(queries_df["id"],queries_df["year"],npv_df["npv"])

    final_df = joined_df.fillna(value=0 , subset=["npv"])

    joined_df.show()
    final_df.show()




























