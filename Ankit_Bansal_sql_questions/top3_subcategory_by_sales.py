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
    # sql query :   select o.sub_category,sum(o.sales) as total_sales
                    # from orders o
                    # inner join returns r
                    # on o.order_id = r.order_id
                    # where o.region = 'East'
                    # group by sub_category
                    # order by total_sales desc
                    # limit 3

    df = orders_df.join(return_df, orders_df["order_id"] == return_df["order_id"],how= "inner")\
        .where(orders_df["region"] == 'East')

    grouped_df = df.groupby(df["sub_category"])\
        .agg(sum(df["sales"]).alias("return_sales"))

    final_df = grouped_df.select(grouped_df["sub_category"],grouped_df["return_sales"])\
        .orderBy(desc(grouped_df["return_sales"])).limit(3)

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


