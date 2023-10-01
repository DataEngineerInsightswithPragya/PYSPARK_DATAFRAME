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

    employee_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='employee_1303',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    employee_df.printSchema()
    employee_df.show()



    #sql
    # select
    #     employee_id,
    #     count(team_id) over(partition by team_id) as team_size
    # from employee

    # jab bhi humhe pure row dikhna hai toh partition by used karenge.

    windowSpec = Window.partitionBy("team_id")
    final_df = employee_df.withColumn("team_size",count("team_id").over(windowSpec)).\
        select("employee_id","team_size")
    final_df.show()


























