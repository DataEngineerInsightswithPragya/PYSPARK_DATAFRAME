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

    rides_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='rides_1407',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    rides_df.printSchema()
    rides_df.show()

    users_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='users_1407',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    users_df.printSchema()
    users_df.show()

    #sql
    # select
    #     name,
    #     sum(coalesce(distance, 0)) as travelled_distance
    # from users left
    # join rides on users.id = rides.user_id
    # group by users.id
    # order by travelled_distance desc, name asc

    # joined_df = users_df.join(rides_df, users_df["id"] == rides_df["user_id"],"left").\
    #     groupby(users_df["id"]).\
    #     agg(sum("distance").alias("travelled_distance")).\
    #     select(users_df["name"]).orderBy(col("travelled_distance").desc() , col("name").asc())

    joined_df = users_df.join(rides_df, users_df["id"] == rides_df["user_id"], "left"). \
        groupby(users_df["id"]). \
        agg(sum("distance").alias("travelled_distance"))

    result_df = joined_df.\
        join(users_df,joined_df["id"] == users_df["id"]).select("name","travelled_distance").\
        fillna(value=0,subset=["travelled_distance"]).\
        orderBy(col("travelled_distance").desc(), col("name").asc())

    joined_df.show()
    result_df.show()


























