from pyspark.sql import SparkSession
from pyspark.sql.types import StructType , StructField , StringType

from sys import stdin

if __name__ == "__main__":
    spark = SparkSession.builder.appName("df").getOrCreate()

    df1 = spark.read.csv("C:/Users/Windows 10/PycharmProjects/learnSpark/Dataset/zipcodes.csv")

    df1.printSchema()

    df1.show()

    df2 = spark.read.option("header",True).csv("C:/Users/Windows 10/PycharmProjects/learnSpark/Dataset/zipcodes.csv")

    df2.printSchema()

    df2.show()

    df3 = spark.read.options(header='True', delimiter=',') \
        .csv("C:/Users/Windows 10/PycharmProjects/learnSpark/Dataset/zipcodes.csv")
    df3.printSchema()

    schema = StructType([ StructField("RecordNumber",StringType(),True), \
                          StructField("Zipcode", StringType(), True), \
                          StructField("ZipCodeType", StringType(), True), \
                          StructField("City", StringType(), True), \
                          StructField("State", StringType(), True), \
                          StructField("LocationType", StringType(), True), \
                          StructField("Lat", StringType(), True), \
                          StructField("Long", StringType(), True), \
                          StructField("Xaxis", StringType(), True), \
                          StructField("Yaxis", StringType(), True), \
                          StructField("Zaxis", StringType(), True), \
                          StructField("WorldRegion", StringType(), True), \
                          StructField("Country", StringType(), True), \
                          StructField("LocationText", StringType(), True), \
                          StructField("Location", StringType(), True), \
                          StructField("Decommisioned", StringType(), True), \
                          StructField("EstimatedPopulation", StringType(), True), \
                          StructField("TotalWages", StringType(), True), \
                          StructField("Notes", StringType(), True)

                          ])

    df4 = spark.read.format("csv")\
            .option("header",True)\
            .schema(schema)\
            .load("C:/Users/Windows 10/PycharmProjects/learnSpark/Dataset/zipcodes.csv")

    df4.printSchema()

    # df2.write.mode('overwrite').csv("C:/Users/Windows 10/PycharmProjects/learnSpark/Dataset/zipcodes_2/f2")
    # df2.to_csv("C:/Users/Windows 10/PycharmProjects/learnSpark/Dataset/zipcodes_2/f2.csv")
    # df4.write.option("header", True) \
    #     .csv("C:/Users/Windows 10/PycharmProjects/learnSpark/Dataset/zipcodes_1/f1")
    df4
