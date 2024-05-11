#from pyspark import F
from pyspark.sql import SparkSession
import findspark
from pyspark.sql.functions import col,sum,min,max

findspark.add_packages('org.postgresql:postgresql:42.2.22')
from sys import stdin


if __name__ == '__main__':

    spark = SparkSession.builder.appName("first_order_date").getOrCreate()

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
    # sql query : select category, sum(profit),min(order_date),max(order_date) from orders group by category

    df = order_df.groupby(order_df['category'])\
        .agg(sum(order_df['profit']).alias("total_profit"),
             min(order_df['order_date']).alias("first_order_date"),
             max(order_df['order_date']).alias("latest_order_date"))


    df.show()
    #print(df.count())
    #print("result : ",result)

    stdin.readline()


