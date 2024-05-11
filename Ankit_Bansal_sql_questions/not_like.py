from pyspark.sql import SparkSession
import findspark
from pyspark.sql.functions import col

findspark.add_packages('org.postgresql:postgresql:42.2.22')
from sys import stdin


if __name__ == '__main__':

    spark = SparkSession.builder.appName("not like").getOrCreate()

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
    # sql query : select * from orders where customer_name not like 'A%n'

    df = order_df.select("*").where(~ order_df["customer_name"].like('A%n'))
    df.show()
    # print(df.count())
    #print("result : ",result)
    print(df.rdd.getNumPartitions())

    stdin.readline()


