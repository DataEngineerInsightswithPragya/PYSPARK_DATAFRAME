from pyspark.sql import SparkSession
import findspark
from pyspark.sql import functions as F

from pyspark.sql.functions import countDistinct
from pyspark.sql.types import StructType, StringType, IntegerType

findspark.add_packages('org.postgresql:postgresql:42.2.22')
from pyspark.sql.functions import min, max, sum, count, col, abs, when, concat, avg, round, dense_rank
from pyspark.sql.window import Window
from pyspark.sql.functions import rank


if __name__ == '__main__':
    # Configure SparkSession
    spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

    # schema = StructType()\
    #     .add("name",StringType(),True)\
    #     .add("age",IntegerType(),True)\
    #     .add("location",StringType(),True)

    age_df = spark.read.format("csv") \
        .option("header", True) \
        .load("C:/Users/Windows 10/PycharmProjects/learnSpark/Dataset/age.csv")
        # .schema(schema) \
        # .load("C:/Users/Windows 10/PycharmProjects/learnSpark/Dataset/age.csv")

    age_df1 = age_df.toDF("name","age","location")

    # age_df = spark.read.csv("C:/Users/Windows 10/PycharmProjects/learnSpark/Dataset/age.csv")\
    #     .option("header", True) \
    #     .schema(schema)

    age_df1.printSchema()
    age_df1.show()

    new_age_df = age_df1.withColumn("greater_than_18",when(age_df1["age"] > 18 ,"YES")
                                   .otherwise("NO"))

    new_age_df.show()