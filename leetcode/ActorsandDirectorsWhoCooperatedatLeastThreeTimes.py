from pyspark.sql import SparkSession
import findspark
from pyspark.sql import functions as F

findspark.add_packages('org.postgresql:postgresql:42.2.22')
from pyspark.sql.functions import min, max, sum, count, col,abs,when,concat
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

import sys

if __name__ == '__main__':
    # Configure SparkSession
    spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
    # tableName = sys.argv[0]

    actor_director_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='actor_director_1050',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    actor_director_df.printSchema()
    actor_director_df.show()

    # SQL QUERY :
    # select
    #     actor_id,
    #     director_id
    #     # count(actor_id) as count_tot
    #     # COUNT(CONCAT(actor_id,director_id)) as count_tot
    # from ActorDirector group by actor_id, director_id
    # having COUNT(CONCAT(actor_id, director_id)) >= 3

    # actor_director_df = actor_director_df.withColumn("concat_col",concat("actor_id", "director_id"))
    #
    # actor_director_df.show()
    actor_director_df = actor_director_df.groupBy("actor_id","director_id").count()

    # +--------+-----------+-----+
    # | actor_id | director_id | count |
    # +--------+-----------+-----+
    # | 1 | 2 | 2 |
    # | 1 | 1 | 3 |
    # | 2 | 1 | 2 |
    # +--------+-----------+-----+

    actor_director_df = actor_director_df.where(actor_director_df["count"] >= 3).select("actor_id","director_id")

    # +--------+-----------+
    # | actor_id | director_id |
    # +--------+-----------+
    # | 1 | 1 |
    # +--------+-----------+

    actor_director_df.show()



