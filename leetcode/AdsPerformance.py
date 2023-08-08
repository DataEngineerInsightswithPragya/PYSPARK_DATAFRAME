from pyspark.sql import SparkSession
import findspark
from pyspark.sql import functions as F

from pyspark.sql.functions import countDistinct

findspark.add_packages('org.postgresql:postgresql:42.2.22')
from pyspark.sql.functions import min, max, sum, count, col, abs, when, concat, avg, round, dense_rank
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

import sys

if __name__ == '__main__':
    # Configure SparkSession
    spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
    # tableName = sys.argv[0]

    ads_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='ads_1322',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    ads_df.printSchema()
    ads_df.show()


    # SQL QUERY :
    # with intermediate_result as (
    #     select
    #         ad_id,
    #         sum(
    #             case
    #                 when action = "Clicked" then 1
    #                 else 0
    #             end) as clicked_action,
    #         sum(
    #             case
    #                 when action = "Viewed" then 1
    #                 else 0
    #             end) as viewed_action
    #     from ads
    #     group by ad_id
    # )
    # select
    #     ad_id,
    #     case
    #         when(sum(clicked_action) + sum(viewed_action)) = 0 then 0
    #         else round((sum(clicked_action) * 100) / (sum(clicked_action) + sum(viewed_action)), 2)
    #     end as ctr
    # from intermediate_result
    # group by ad_id
    # order by ctr desc, ad_id

    intermediate_df = ads_df.groupBy("ad_id").\
        agg(
            sum(when(col("action") == "Clicked",1).otherwise(0)).alias("clicked_action"),
            sum(when(col("action") == "Viewed",1).otherwise(0)).alias("viewed_action")
        )


    result_df = intermediate_df.groupBy("ad_id").\
        agg(
            when(sum(col("clicked_action")) + sum(col("viewed_action")) == 0 ,0)
            .otherwise(
                round(
                    sum(
                        col("clicked_action")
                    ) * 100/
                    (sum(
                        col("clicked_action")
                    ) +
                    sum(
                        col("viewed_action")
                    )),2
                )).alias("ctr")
        )\
         .orderBy(col("ctr").desc() , col("ad_id").asc())


    intermediate_df.show()
    print("************")
    result_df.show()




















