from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from sys import stdin
# Create Spark session
spark = SparkSession.builder.appName("test").getOrCreate()

# test_df = spark.read.format("csv")\
#     .option("header",True)\
#     .option("inferSchema",True)\
#     .load("C:/Users/Windows 10/PycharmProjects/learnSpark/Dataset/test.csv")


test_df = spark.read.option("header",True)\
     .option("inferSchema",True)\
    .csv("C:/Users/Windows 10/PycharmProjects/learnSpark/Dataset/test.csv")\


    # .option("inferSchema",True)\
    #.load()

test_df.show()

#test_df.explain()

stdin.readline()