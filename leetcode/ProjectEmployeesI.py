from pyspark.sql import SparkSession
import findspark
from pyspark.sql import functions as F

findspark.add_packages('org.postgresql:postgresql:42.2.22')
from pyspark.sql.functions import min, max, sum, count, col,abs,when,concat,avg,round
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
    # select
    #     project_id,
    #     # project.employee_id,
    #     # experience_years ,
    #     round(avg(experience_years), 2) as average_years
    # from project join employee on project.employee_id = employee.employee_id group by project_id

    joined_df = project_df.join(employee_df,project_df["employee_id"] == employee_df["employee_id"])\
        .groupBy("project_id").agg(avg("experience_years").alias("average_years"))
    #average_years_df = joined_df.withColumn("average_years",agg(avg("experience_years")))
    # joined_df = joined_df.select("project_id").
    joined_df.show()








