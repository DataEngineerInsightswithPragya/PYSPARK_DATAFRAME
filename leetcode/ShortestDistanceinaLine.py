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

    point_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='point_613',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    point_df.printSchema()
    point_df.show()

    # SQL QUERY :
    # select
    #     min(abs((a.x) - (b.x))) as shortest
    # from point a join point b where a.x != b.x

    point_df = point_df.alias("a").join(point_df.alias("b"))
    point_df = point_df.withColumn("Shortest",(point_df["a.x"]- point_df["b.x"])).where(point_df["a.x"] != point_df["b.x"])
    point_df = point_df.select(min(abs("Shortest")))
    point_df.show()
    #select(min(abs("a.x" - "b.x")))




