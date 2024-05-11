from pyspark.sql import SparkSession
import findspark
findspark.add_packages('org.postgresql:postgresql:42.2.22')
from sys import stdin


if __name__ == '__main__':

    spark = SparkSession.builder.appName("find_customer_name").getOrCreate()

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

    # sql query : select * from orders where order_date >= '2020-12-01' and order_date <= '2020-12-31'

    df = order_df.select("*")\
        .where((order_df['order_date'] >= '2020-12-01') & (order_df['order_date'] <= '2020-12-31'))
    df.show()

    spark.stop()
