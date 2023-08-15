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

    prices_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='prices_1251',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    unitsold_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='unit_sold_1251',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    prices_df.printSchema()
    prices_df.show()

    unitsold_df.printSchema()
    unitsold_df.show()


    # SQL QUERY :
    # SELECT
    #     p.product_id,
    #     round(sum((p.price * u.units)) / sum(u.units), 2) as average_price
    # from Prices p join UnitsSold u
    # on p.product_id = u.product_id and \
    # u.purchase_date between p.start_date and end_date
    # group by p.product_id

    joined_df = prices_df.join(unitsold_df,(prices_df["product_id"] == unitsold_df["product_id"])
                & (unitsold_df["purchase_date"].between(prices_df["start_date"],prices_df["end_date"])))

    # result_df = joined_df.groupBy(prices_df["product_id"])\
    #     .agg(round(sum(prices_df["price"] * unitsold_df["units"]))/sum(unitsold_df["units"]),2)\
    #     .alias("average_price")

    average_price_df = joined_df.groupBy(prices_df["product_id"])\
        .agg(
                round(sum(prices_df["price"] * unitsold_df["units"]) / sum(unitsold_df["units"]), 2)
        .alias("average_price")
        )
    joined_df.show()
    average_price_df.show()










