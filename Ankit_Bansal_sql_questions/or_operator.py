from pyspark.sql import SparkSession
import findspark
from pyspark.sql.functions import col

findspark.add_packages('org.postgresql:postgresql:42.2.22')
from sys import stdin


if __name__ == '__main__':

    spark = SparkSession.builder.appName("or_operator").getOrCreate()

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
    # sql query : select * from orders where quantity < 3 or profit = 0

    df = order_df.select("*").where((order_df["quantity"] < 3) | (order_df['profit'] == 0))
    df.show()
    print(df.count())
    #print("result : ",result)

    stdin.readline()


