#from pyspark import F
from pyspark.sql import SparkSession
import findspark
from pyspark.sql.functions import col, sum, min, max, avg

findspark.add_packages('org.postgresql:postgresql:42.2.22')
from sys import stdin


if __name__ == '__main__':

    spark = SparkSession.builder.appName("sub_category").getOrCreate()

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
    # sql query : select sub_category,avg(profit),max(profit) from orders
                # group by sub_category
                # having avg(profit) > 0.5 * max(profit)

    df = order_df.groupby(order_df['sub_category'])\
        .agg(avg(order_df['profit']).alias("avg_profit"),
             max(order_df['profit']).alias("max_profit"))\
        .where(avg(order_df['profit']) > (0.5 * max(order_df['profit'])))



    df.show()
    #print(df.count())
    #print("result : ",result)

    stdin.readline()


