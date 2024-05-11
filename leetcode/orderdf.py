from pyspark.sql import SparkSession
from sys import stdin

if __name__ == "__main__":

    spark = SparkSession.builder.master("local[*]")\
            .appName("orderdf")\
            .getOrCreate()

    df = spark.read.option("header",True)\
        .option("inferSchema",True)\
        .csv("C:/Users/Windows 10/PycharmProjects/learnSpark/Dataset/order_schema.csv")

    print(df.rdd.getNumPartitions())

    df.printSchema()
    df.show()

    df.write.option("header", True) \
        .csv("C:/Users/Windows 10/PycharmProjects/learnSpark/Dataset/order_schema2.csv")

    stdin.readline()


