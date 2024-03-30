# Import PySpark
from pyspark.sql import SparkSession

#Create SparkSession
def createDataframe():
    spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
    data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]

    # Columns
    columns = ["language", "users_count"]

    # Create DataFrame
    df = spark.createDataFrame(data).toDF(*columns)

    # Print DataFrame
    df.show()


if __name__ == '__main__':



    createDataframe()


# Data

