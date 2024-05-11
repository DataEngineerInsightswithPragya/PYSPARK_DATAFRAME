#from pyspark import F
from pyspark.sql import SparkSession
import findspark
from pyspark.sql.functions import col, sum, min, max, avg, count, countDistinct, desc

findspark.add_packages('org.postgresql:postgresql:42.2.22')
from sys import stdin


if __name__ == '__main__':

    spark = SparkSession.builder.appName("sub_category_west").getOrCreate()

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
    # sql query : select sub_category,sum(quantity) from orders
                    # where region = 'West'
                    # group by sub_category
                    # order by sum(quantity) desc
                    # limit 5

    df = order_df.where(order_df['region'] == 'West').\
        groupby(order_df['sub_category']).\
        agg(sum(order_df['quantity']).alias("total_quantity")).\
        orderBy(desc(sum(order_df['quantity']))).limit(5)



    df.show()
    #print(df.count())
    #print("result : ",result)

    stdin.readline()


