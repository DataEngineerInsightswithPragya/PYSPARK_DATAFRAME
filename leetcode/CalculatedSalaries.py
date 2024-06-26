from pyspark.sql import SparkSession
import findspark
from pyspark.sql import functions as F

from pyspark.sql.functions import countDistinct, expr, month, concat_ws, collect_list, collect_set, sort_array

findspark.add_packages('org.postgresql:postgresql:42.2.22')
from pyspark.sql.functions import min, max, sum, count, col, abs, when, concat, avg, round, dense_rank
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

import sys


if __name__ == '__main__':
    # Configure SparkSession
    spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
    # tableName = sys.argv[0]

    salaries_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='salaries_1468',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    salaries_df.printSchema()
    salaries_df.show()

    #sql query : with cte as (select *,max(salary) over(partition by company_id) as max_salary from salaries)
                # select
                #     company_id,
                #     employee_id,
                #     employee_name,
                #     case
                #         when max_salary < 1000 then salary
                #         when max_salary >= 1000 and max_salary <= 10000
                #         then round(salary - (24/100) * salary)
                #         when (max_salary > 10000 ) then round(salary - (49/100) * salary)
                #     end as salary
                #
                # from cte

    windowSpec = Window.partitionBy(salaries_df["company_id"])

    df = salaries_df.select("*")\
        .withColumn("max_salary", max(salaries_df["salary"]).over(windowSpec).alias("max_salary"))

    df.show()
    df.printSchema()

    final_df = df.withColumn("salary",
                    when(df["max_salary"] < 1000,df["salary"])\
                    .when((df["max_salary"] >= 1000) & (df["max_salary"] <= 10000),
                          round(df["salary"] - (24/100) * df["salary"]))\
                    .when((df["max_salary"] > 10000),round(df["salary"] - (49/100) * df["salary"]))\
                    .otherwise(df["salary"])
                    ).select(df["company_id"],df["employee_id"],df["employee_name"],col("salary"))



    final_df.show()

    sys.stdin.readline()


























