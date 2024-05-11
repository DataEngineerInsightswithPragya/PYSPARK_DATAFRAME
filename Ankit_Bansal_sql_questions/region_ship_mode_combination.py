#from pyspark import F
from pyspark.sql import SparkSession
import findspark
from pyspark.sql.functions import col, sum, min, max, avg, count, countDistinct, desc

findspark.add_packages('org.postgresql:postgresql:42.2.22')
from sys import stdin


if __name__ == '__main__':

    spark = SparkSession.builder.appName("region_ship_mode_combination").getOrCreate()

    print("spark  : ",spark)

    order_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='orders',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    order_df.printSchema()
    # sql query :   select region,ship_mode,sum(sales) from orders
                    # where order_date >= '2020-01-01' and order_date < '2021-01-01'
                    # group by region,ship_mode

    df = order_df.where((order_df['order_date'] >= '2020-01-01') & (order_df['order_date'] < '2021-01-01'))\
        .groupby(order_df['region'],order_df['ship_mode']).agg(sum(order_df['sales']).alias("total_sales"))



    df.show(truncate=False)
    #print(df.count())
    #print("result : ",result)

    stdin.readline()


