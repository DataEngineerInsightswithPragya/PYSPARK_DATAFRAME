from pyspark.sql import SparkSession, window, Window
import findspark
from pyspark.sql.functions import dense_rank, col

findspark.add_packages('org.postgresql:postgresql:42.2.22')
from sys import stdin


if __name__ == '__main__':

    spark = SparkSession.builder.appName("The_Most_Recent_Three_Orders").getOrCreate()

    print("spark  : ",spark)

    customer_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='customers_1532',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    customer_df.printSchema()


    order_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='orders_1532',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    order_df.printSchema()

    # sql query : with cte as (select
    #     c.name as customer_name,
    #     c.customer_id,
    #     o.order_id,
    #     o.order_date,
    #     dense_rank() over(partition by o.customer_id order by o.order_date desc) as rnk
    # from orders o join customers c on o.customer_id = c.customer_id)
    # select
    #     customer_name,
    #     customer_id,
    #     order_id,
    #     order_date
    # from cte where rnk <=3
    # order by customer_name,customer_id,order_date desc

    windowSpec = Window.partitionBy(order_df["customer_id"]).orderBy(order_df["order_date"].desc())

    df = order_df.join(customer_df,order_df["customer_id"] == customer_df["customer_id"]) \
        .withColumn("rnk", dense_rank().over(windowSpec))\
        .select(customer_df["name"].alias("customer_name"),customer_df["customer_id"],
                 order_df["order_id"],order_df["order_date"],col("rnk"))\


    df.show()

    final_df = df.select(df["customer_name"],df["customer_id"],df["order_id"],df["order_date"])\
        .where(df["rnk"] <= 3).orderBy(df["customer_name"],df["customer_id"],df["order_date"].desc())

    final_df.show()







    #print(rdd.toDebugString())


    stdin.readline()

