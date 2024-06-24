#from pyspark import F
from pyspark.sql import SparkSession
import findspark
from pyspark.sql.functions import col, sum, min, max, avg, count, countDistinct, desc, month

findspark.add_packages('org.postgresql:postgresql:42.2.22')
from sys import stdin


if __name__ == '__main__':

    spark = SparkSession.builder.appName("dept_with_no_employee").getOrCreate()

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

    #question : 1- write a query to find subcategories who never had any return orders in the month of november
                # (irrespective of years)

    # sql query :   select sub_category
                    # from orders o
                    # left join returns r on o.order_id=r.order_id
                    # where month(order_date)=11
                    # group by sub_category
                    # having count(r.order_id)=0;



    df = order_df.join(return_df,order_df["order_id"] == return_df["order_id"],how="left")\
        .where(month(order_df["order_date"]) == 11)\
        .groupby(order_df["sub_category"].alias("sub_category"))\
        .agg(count(return_df["order_id"]).alias("count_of_return_order"))


    final_df = df.select(df["sub_category"]).where(df["count_of_return_order"] == 0)




    # To check the Broadcast Join Threshold
    broadcast_threshold =spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    print('broadcast_threshold : ',broadcast_threshold)

    df.show(truncate=False)
    final_df.show(truncate=False)

    #print(df.count())
    #print("result : ",result)

    stdin.readline()


