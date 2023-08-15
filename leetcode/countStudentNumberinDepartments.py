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

    student_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='student_580',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    student_df.printSchema()
    student_df.show()



    department_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='department_580',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    department_df.printSchema()
    department_df.show()


    # SQL QUERY :
    # select
    #     dept_name,
    #     count(student_id) as student_number
    # from student right join department on student.dept_id = department.dept_id
    # group by department.dept_name
    # order by student_number desc, dept_name

    joined_df = student_df.\
        join(department_df,student_df["dept_id"] == department_df["dept_id"],"right")
        # count(col("student_id").alias("student_number"))

    intermediate_df = joined_df.groupBy(department_df["dept_name"]).\
        agg(count(col("student_id")).alias("student_number")).\
        orderBy(col("student_number").desc(),department_df["dept_name"])


    joined_df.show()
    intermediate_df.show()
    



















