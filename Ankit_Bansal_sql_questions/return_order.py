#from pyspark import F
from pyspark.sql import SparkSession
import findspark
from pyspark.sql.functions import col, sum, min, max, avg, count, countDistinct, desc

findspark.add_packages('org.postgresql:postgresql:42.2.22')
from sys import stdin


if __name__ == '__main__':

    spark = SparkSession.builder.appName("return_order").getOrCreate()

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


    return_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='returns',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    order_df.printSchema()
    return_df.printSchema()
    # sql query :   select region,count(distinct o.order_id) as no_of_return_orders
                    # from orders o
                    # inner join returns r on o.order_id=r.order_id
                    # group by region

    df = order_df.join(return_df,order_df['order_id']== return_df['order_id'],how='inner')\
        .groupby(order_df['region'])\
        .agg(countDistinct(order_df['order_id']).alias("no_of_return_orders"))


    # To check the Broadcast Join Threshold
    broadcast_threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    print('broadcast_threshold : ',broadcast_threshold)

    df.show(truncate=False)
    #print(df.count())
    #print("result : ",result)

    stdin.readline()


