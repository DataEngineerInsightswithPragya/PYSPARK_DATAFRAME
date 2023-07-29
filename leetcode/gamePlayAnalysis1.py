from pyspark.sql import SparkSession
import findspark
findspark.add_packages('org.postgresql:postgresql:42.2.22')
from pyspark.sql.functions import min, max
import sys


if __name__ == '__main__':
    # Configure SparkSession
    spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
    #tableName = sys.argv[0]

    activity_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='activity_511',
        #dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    activity_df.printSchema()
    activity_df.show()

    #SQL QUERY :
    # select
    #     player_id ,
    #     min(event_date) as first_login
    # from activity
    # group by player_id;

    # Perform the grouping and aggregation
    result_df = activity_df.groupBy("player_id").agg(min("event_date").alias("first_login"))
    result_df.show()





