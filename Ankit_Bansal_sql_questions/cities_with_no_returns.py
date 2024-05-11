#from pyspark import F
from pyspark.sql import SparkSession
import findspark
from pyspark.sql.functions import col, sum, min, max, avg, count, countDistinct, desc

findspark.add_packages('org.postgresql:postgresql:42.2.22')
from sys import stdin


if __name__ == '__main__':

    spark = SparkSession.builder.appName("cities_with_no_return_reason").getOrCreate()

    print("spark  : ",spark)

    orders_df = spark.read.format("jdbc"). \
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

    orders_df.printSchema()
    return_df.printSchema()
    # sql query :   select city
                    # from orders o
                    # left join returns r on o.order_id=r.order_id
                    # group by city
                    # having count(r.order_id)=0

    df = orders_df.join(return_df, orders_df["order_id"] == return_df["order_id"],how= "left")

    grouped_df = df.groupby(df["city"])\
        .agg(countDistinct(df["return_reason"]).alias("no_of_return_reason"))

    final_df = grouped_df.select(grouped_df["city"]).where(grouped_df["no_of_return_reason"] == 0)

    result = final_df.count()



    # To check the Broadcast Join Threshold
    broadcast_threshold =spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    print('broadcast_threshold : ',broadcast_threshold)

    df.show(truncate=False)
    grouped_df.show(truncate=False)
    final_df.show(truncate=False)
    print("result : ",result)
    #print(df.count())
    #print("result : ",result)

    stdin.readline()


