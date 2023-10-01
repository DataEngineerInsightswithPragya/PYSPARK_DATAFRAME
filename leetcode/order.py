from datetime import datetime

from pyspark.sql import SparkSession
import findspark
from pyspark.sql import functions as F

from pyspark.sql.functions import countDistinct, unix_timestamp, monotonically_increasing_id
from pyspark.sql.types import StructType, StringType, IntegerType, StructField, DateType

findspark.add_packages('org.postgresql:postgresql:42.2.22')
from pyspark.sql.functions import min, max, sum, count, col, abs, when, concat, avg, round, dense_rank
from pyspark.sql.window import Window
from pyspark.sql.functions import rank


if __name__ == '__main__':
    # Configure SparkSession
    spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

    # creating a list
    order = [(1,'2013-07-25',11599,"closed"),
            (2,'2014-07-25',256,"pending_payment"),
            (3,'2013-07-25',11599,"complete"),
            (4,'2019-07-25',8827,"closed")
        ]

    print("orders list ",order)

    #order_column = ["orderid"]

    order_schema = StructType([
        StructField("orderid",IntegerType(),True),
        StructField("orderdate",StringType(),True),
        StructField("customer_id",IntegerType(),True),
        StructField("status",StringType(),True)
    ])


    order_df = spark.createDataFrame(data = order , schema = order_schema )
    order_df.printSchema()
    order_df.show()

    unix_order_df = order_df.select(col("orderid"),
                                    unix_timestamp(col("orderdate"),"yyyy-MM-dd").alias("orderdate"),
                                    col("customer_id"),
                                    col("status"))
    # unix_order_df.show()

    # Add a new column "new_id" with unique IDs
    unix_order_df = unix_order_df.withColumn("new_id", monotonically_increasing_id())
    unix_order_df = unix_order_df.dropDuplicates(["orderdate","customer_id"])
    # dropDisDF = df.dropDuplicates(["department", "salary"])
    unix_order_df = unix_order_df.drop("orderid")

    unix_order_df.show()



