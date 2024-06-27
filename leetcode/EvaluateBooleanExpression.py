from pyspark.sql import SparkSession
import findspark
from pyspark.sql import functions as F

from pyspark.sql.functions import countDistinct, expr, month, concat_ws, collect_list, collect_set, sort_array, lead

findspark.add_packages('org.postgresql:postgresql:42.2.22')
from pyspark.sql.functions import min, max, sum, count, col, abs, when, concat, avg, round, dense_rank
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

import sys


if __name__ == '__main__':
    # Configure SparkSession
    spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
    # tableName = sys.argv[0]

    variables_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='variables_1440',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    variables_df.printSchema()
    variables_df.show()

    expressions_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='expressions_1440',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    expressions_df.printSchema()
    expressions_df.show()

    #sql query : SELECT
                #     e.left_operand,
                #     e.operator,
                #     e.right_operand,
                #     case
                #         when e.operator = '>' and v1.value > v2.value then 'true'
                #         when e.operator = '=' and v1.value = v2.value then 'true'
                #         when e.operator = '<' and v1.value < v2.value then 'true'
                #         else 'false'
                #     end as value
                # from expressions e
                # join variables v1 on e.left_operand = v1.name
                # join variables v2 on e.right_operand = v2.name

    # Alias DataFrames
    e = expressions_df.alias("e")
    v1 = variables_df.alias("v1")
    v2 = variables_df.alias("v2")


    # joined_df = e.join(v1,e["left_operand"] == (v1["name"]).alias("v1_val"))\
    # .join(v2,e["right_operand"] == (v2["name"]).alias("v2_val"))
    #     #.select(e["left_operand"],e["operator"],e["right_operand"],col("v1_val"),col("v2_val"))

    joined_df1 = e.join(v1, e["left_operand"] == v1["name"])\
        .select(
        e["left_operand"],
        e["operator"],
        e["right_operand"],
        v1["value"].alias("v1_val")
    )
    joined_df1.show()

    joined_df2 = joined_df1.join(v2, joined_df1["right_operand"] == v2["name"]) \
        .select(
        joined_df1["left_operand"],
        joined_df1["operator"],
        joined_df1["right_operand"],
        joined_df1["v1_val"],
        col("value").alias("v2_val")
    )

    joined_df2.show()


    final_df = joined_df2.withColumn("value",when((col("operator") == '>') & (col("v1_val") > col("v2_val")) ,'true')
                                     .when((col("operator") == '<') & (col("v1_val") < col("v2_val")) ,'true')
                                     .when((col("operator") == '=') & (col("v1_val") == col("v2_val")) ,'true')
                                     .otherwise("false"))\
        .select(
        col("left_operand"),
        col("operator"),
        col("right_operand"),
        col("value")
    )

    final_df.show()

    sys.stdin.readline()


























