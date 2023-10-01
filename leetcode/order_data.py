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

    invoice_df = spark.read\
        .format("csv")\
        .option("header",True)\
        .option("inferSchema" ,True)\
        .option("path","C:/Users/Windows 10/PycharmProjects/learnSpark/Dataset/order_data-201025-223502.csv")\
        .load()
    invoice_df.show()

    rows_count = invoice_df.count()
    print("rows_count ", rows_count)

    quantity_df = invoice_df.select("*")
    quantity_df.show()
    quantity_df.select(sum(quantity_df["Quantity"]).alias("quantity")).show()
    quantity_df.show()
    quantity_df.select(avg(quantity_df["UnitPrice"]).alias("avg_unit_price")).show()


    #print("Quantity_sum ", Quantity_sum)


    group_df = invoice_df.groupBy(invoice_df["InvoiceNo"],invoice_df["Country"])\
        .agg(sum(invoice_df["Quantity"]).alias("total_quantity") ,
             sum(invoice_df["Quantity"] * invoice_df["UnitPrice"]).alias("invoice_value")
             )
    print("**************")
    group_df.show()










