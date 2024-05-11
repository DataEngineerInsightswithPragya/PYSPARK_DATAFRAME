from pyspark.sql import SparkSession
from pyspark.sql.types import StructType , StructField , StringType

from sys import stdin

if __name__ == "__main__":
    spark = SparkSession.builder.appName("df").getOrCreate()

    data = [('hr','priya'),('it','piu'),('it','sunny'),('hr','rani')]

    schema = ['department','name']

    df = spark.createDataFrame(data,schema)
    df.show()

    df.groupBy("department").show()
