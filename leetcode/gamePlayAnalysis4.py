from pyspark.sql import SparkSession
import findspark
findspark.add_packages('org.postgresql:postgresql:42.2.22')
from pyspark.sql.functions import min, max, sum, datediff, col, countDistinct
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

import sys


if __name__ == '__main__':
    # Configure SparkSession
    spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
    #tableName = sys.argv[0]

    activity_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='activity_550',
        #dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    activity_df.printSchema()
    activity_df.show()

    #SQL QUERY :
    # with intermediate_result_1 as (
    #         select
    #         a.*,
    #         min(event_date) over(partition by player_id ) as min_date
    # from activity as a
    # ),
    # intermediate_result_2 as (
    #     select
    #         count(distinct(player_id)) as player_count
    #     from intermediate_result_1
    #     where datediff(intermediate_result_1.event_date, intermediate_result_1.min_date) = 1
    # # group by player_id
    # )
    # select
    # round((intermediate_result_2.player_count) / (select count(distinct(player_id))
    # from activity), 2) as fraction
    # from intermediate_result_2

    # Define the window specification
    windowSpec = Window.partitionBy("player_id")
    print("windowSpec ---> ",dir(windowSpec))

    # Add a new column "rank_row" that represents the rank of each row within the partition
    intermediate_result_1  = activity_df.withColumn("min_date ",min("event_date").over(windowSpec))
    intermediate_result_1.show()
    # date_diff = datediff("event_date","min_date")
    # intermediate_result_2 = intermediate_result_1 \
    #     .where(date_diff == 1) \
    #     .agg(countDistinct("player_id").alias("player_count"))
    #datediff_col = datediff("event_date", col("min_date"))  # Recompute datediff here
    #intermediate_result_2 = intermediate_result_1.filter(datediff('event_date', col('min_date')) == 1).select("player_id")

    #intermediate_result_2.show()
    #activity_df = activity_df.filter(activity_df.)


















