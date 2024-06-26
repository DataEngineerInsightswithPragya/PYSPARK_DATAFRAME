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

    sales_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='sales_1445',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    sales_df.printSchema()
    sales_df.show()

    #sql query : with cte as (select
                #     *,
                #     lead(sold_num) over(partition by sale_date order by fruit) as lead_sold_num
                # from sales
                # )
                # select
                #     sale_date,
                #     (sold_num - lead_sold_num) as diff
                # from cte
                # group by sale_date
                # order by sale_date

    windowSpec = Window.partitionBy(sales_df["sale_date"]).orderBy(sales_df["fruit"])

    df = sales_df.withColumn("lead_sold_num", lead(sales_df["sold_num"]).over(windowSpec))\
                .select(sales_df["sale_date"],sales_df["sold_num"],col("lead_sold_num"))

    df.show()

    # final_df = df.groupBy(df["sale_date"],df["sold_num"],df["lead_sold_num"])\
    #     .agg((df["sold_num"]) - (df["lead_sold_num"])).alias("diff")\
    #     .orderBy(df["sale_date"])

    # final_df = df.select(df["sold_num"]) - (df["lead_sold_num"]).alias("diff")).groupBy(df["sale_date"])\
    #     .orderBy(df["sale_date"])

    final_df = df.select(df["sale_date"],(col("sold_num") - col("lead_sold_num")).alias("diff"))\
        .orderBy("sale_date")


    final_result_df = final_df.where(final_df["diff"].isNotNull())

    final_df.show()
    final_result_df.show()

    sys.stdin.readline()


























