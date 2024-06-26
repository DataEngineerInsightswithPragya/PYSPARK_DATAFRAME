from pyspark.sql import SparkSession
import findspark
from pyspark.sql import functions as F

from pyspark.sql.functions import countDistinct, expr, month, collect_set, array_sort

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

    activities_df.show()

    #sql query : select  sell_date,count(distinct product) as num_sold,
                #     group_concat(distinct product order by product separator ',') as products
                # from activities
                # group by sell_date
                # order by sell_date asc


    df = activities_df.groupby(activities_df["sell_date"])\
        .agg(countDistinct(activities_df["product"]).alias("num_sold"),
             array_sort(collect_set(activities_df["product"])).alias("products"))\
        .orderBy(activities_df["sell_date"])

    df.show(truncate=False)


    sys.stdin.readline()


























