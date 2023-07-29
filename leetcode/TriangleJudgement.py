from pyspark.sql import SparkSession
import findspark
from pyspark.sql import functions as F

findspark.add_packages('org.postgresql:postgresql:42.2.22')
from pyspark.sql.functions import min, max, sum, count, col,abs,when
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

import sys

if __name__ == '__main__':
    # Configure SparkSession
    spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
    # tableName = sys.argv[0]

    triangle_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='triangle_610',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    triangle_df.printSchema()
    triangle_df.show()

    # SQL QUERY :
    # select
    #     *,
    #     case
    #         when(x + y > z) & & (y + z > x) & & (z + x > y) then "Yes"
    #         else "No"
    #     end as triangle
    # from Triangle;

    triangle_df = triangle_df.withColumn("triangle",when((triangle_df["x"] + triangle_df["y"] > triangle_df["z"])
        & (triangle_df["z"] + triangle_df["y"] > triangle_df["x"]) & (triangle_df["z"] + triangle_df["y"] > triangle_df["x"])
        ,"Yes")
        .otherwise("No"))
    triangle_df.show()
