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
    # sql query :   select category,sum(sales) from orders left join returns
                    # on orders.order_id = returns.order_id
                    # where returns.order_id is null
                    # group by category

    df = order_df.join(return_df,order_df['order_id'] == return_df['order_id'],how='left')\
        .where(return_df['order_id'].isNull())\
        .groupby(order_df['category'])\
        .agg(sum(order_df['sales']).alias("total_sales"))


    # To check the Broadcast Join Threshold
    broadcast_threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    print('broadcast_threshold : ',broadcast_threshold)

    df.show(truncate=False)
    #print(df.count())
    #print("result : ",result)

    stdin.readline()


