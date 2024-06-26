from pyspark.sql import SparkSession
import findspark
from pyspark.sql import functions as F

from pyspark.sql.functions import countDistinct, expr, month

findspark.add_packages('org.postgresql:postgresql:42.2.22')
from pyspark.sql.functions import min, max, sum, count, col, abs, when, concat, avg, round, dense_rank
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

import sys





if __name__ == '__main__':
    # Configure SparkSession
    spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
    # tableName = sys.argv[0]

    content_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='content_1495',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    content_df.printSchema()
    content_df.show()

    tvprogram_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='tv_program_1495',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    tvprogram_df.printSchema()
    tvprogram_df.show()



    # SQL QUERY : select distinct c.title
    # from content c join tvprogram t
    # on c.content_id = t.content_id
    # where ((t.program_date >= '2020-06-01' and t.program_date <= '2020-06-30')
    # and c.kids_content = 'Y' and c.content_type = 'Movies')

    df = content_df.join(tvprogram_df,content_df["content_id"] == tvprogram_df["content_id"])\
        .where((tvprogram_df["program_date"] >= '2020-06-01') & (tvprogram_df["program_date"] <= '2020-06-30')
               & (content_df["kids_content"] == 'Y') & (content_df["content_type"] == "Movies"))\
        .select(content_df["title"]).distinct()

    df.show()

    sys.stdin.readline()


























