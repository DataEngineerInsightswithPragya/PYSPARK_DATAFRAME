from pyspark.sql import SparkSession
import findspark
from pyspark.sql import functions as F

findspark.add_packages('org.postgresql:postgresql:42.2.22')
from pyspark.sql.functions import min, max, sum, count, col, abs, when, concat, avg, round, dense_rank
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

import sys

if __name__ == '__main__':
    # Configure SparkSession
    spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
    # tableName = sys.argv[0]

    project_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='project_1075',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    employee_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='employee_1075',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    project_df.printSchema()
    project_df.show()

    employee_df.printSchema()
    employee_df.show()


    # SQL QUERY :
    # Write your MySQL query statement below
    # with most_experience as (
    #         select
    #             project_id,
    #             project.employee_id,
    #             experience_years,
    #             dense_rank() over(partition by project_id order by experience_years desc) as dense_rnk
    #         from project join employee on project.employee_id = employee.employee_id
    # )
    # select
    #     project_id,
    #     employee_id
    # from most_experience where dense_rnk = 1

    employee_df = employee_df.withColumnRenamed("employee_id","employeeid")
    employee_df.show()
    joined_df = project_df.join(employee_df,project_df["employee_id"] == employee_df["employeeid"])
    windowSpec = Window.partitionBy("project_id").orderBy(col("experience_years").desc())
    joined_df2 = joined_df.withColumn("dense_rnk", dense_rank().over(windowSpec))
    joined_df2.show()
    joined_df2.printSchema()
    equal_to = 1
    final_df = joined_df2.select("project_id","employeeid").where(joined_df2["dense_rnk"] == 1)
    # joined_df = joined_df.filter(col("dense_rnk") == 1).select("project_id", "employee_id")
    final_df.show()
#





