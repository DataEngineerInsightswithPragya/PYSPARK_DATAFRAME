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
    # with max_employee as (
    #         select
    #         project_id,
    #         count(employee_id) as total
    # from project group by project_id
    # )
    # select
    #     project_id
    # from max_employee where total = (select max(total) from max_employee)

    project_df = project_df.groupBy("project_id").agg(count("employee_id").alias("total"))
    max_total = project_df.agg(F.max("total")).first()[0]
    print("max_total",max_total)
    max_employee = project_df.select("project_id").where(project_df["total"] == max_total)
    max_employee.show()






