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

    cinema_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='cinema_603',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    cinema_df.printSchema()
    cinema_df.show()

    # SQL QUERY :
    # Write your MySQL query statement below
    # select
    #     distinct c1.seat_id
    # # ,c1.free,c2.seat_id,c2.free
    # from cinema c1
    # join cinema c2 on abs(c1.seat_id - c2.seat_id) = 1 and c1.free = 1 and c2.free = 1
    # order by c1.seat_id

    # cinema_df_joined: object
    cinema_df_joined = cinema_df.alias("c1").join(
        cinema_df.alias("c2"),
        (F.abs(F.col("c1.seat_id") - F.col("c2.seat_id")) == 1)
        & (F.col("c1.free") == 1) & (F.col("c2.free") == 1))
    # cinema_df_joined = cinema_df.alias("c1").join(
    #     cinema_df.alias("c2"),
    #     (F.abs(F.col("c1.seat_id") - F.col("c2.seat_id")) == 1) &
    #     (F.col("c1.free") == 1) & (F.col("c2.free") == 1)
    # )

    cinema_df_joined = cinema_df_joined.select("c1.seat_id").distinct().orderBy(col("c1.seat_id").asc())

    cinema_df_joined.show()
