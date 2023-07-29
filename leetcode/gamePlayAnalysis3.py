from pyspark.sql import SparkSession
import findspark
findspark.add_packages('org.postgresql:postgresql:42.2.22')
from pyspark.sql.functions import min, max , sum
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
        dbtable='activity_534',
        #dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    activity_df.printSchema()
    activity_df.show()

    #SQL QUERY :
    # select
    #     player_id,
    #     event_date,
    #     sum(games_played) over(partition by player_id order by event_date) as games_played_so_far
    # from activity




    # Define the window specification
    windowSpec = Window.partitionBy("player_id").orderBy("event_date")
    print("windowSpec ---> ",dir(windowSpec))

    # Add a new column "rank_row" that represents the rank of each row within the partition
    activity_df = activity_df.withColumn("games_played_so_far ",sum("games_played").over(windowSpec))

    # Filter the DataFrame to show only rows where rank_row is equal to 1
    #result_df = activity_df.filter(activity_df.rank_row == 1)

    # Select only the "player_id" and "device_id" columns
    result_df = activity_df.select("player_id", "event_date" , "games_played_so_far ")
    result_df.show()







