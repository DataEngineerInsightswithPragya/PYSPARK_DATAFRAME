from pyspark.sql import SparkSession
import findspark
from pyspark.sql import functions as F

from pyspark.sql.functions import countDistinct, expr

findspark.add_packages('org.postgresql:postgresql:42.2.22')
from pyspark.sql.functions import min, max, sum, count, col, abs, when, concat, avg, round, dense_rank
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

import sys


if __name__ == '__main__':
    # Configure SparkSession
    spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
    # tableName = sys.argv[0]

    tree_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='tree_608',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    tree_df.printSchema()
    tree_df.show()


    # SQL QUERY :
    # select
    #     id,
    #     case
    #         when(p_id is null) then "Root"
    #         when(id in (
    #             select distinct(p_id) from tree where p_id not in (
    #                 select id from tree where p_id is null)))  then "Inner"
    #         else "Leaf"
    #     end as type
    # from tree

    root_list = tree_df.select("id").\
        where(col("p_id").isNull()).rdd.\
        map(lambda row : row[0]).collect()

    inner_list = tree_df.select("p_id").\
        where(~ col("p_id").isin(root_list)).\
        distinct().rdd.map(lambda row : row[0]).collect()

    result_df = tree_df.withColumn("type",when(tree_df["p_id"].isNull(),"Root")
                   .when(tree_df["id"].isin(inner_list) , "Inner")
                   .otherwise("Leaf"))
    print("root_df",root_list)
    print("inner_list" , inner_list)
    result_df.show()





















