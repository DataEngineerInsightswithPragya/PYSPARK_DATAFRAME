from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Assuming you already have a SparkSession
spark = SparkSession.builder.appName("ExcludeValues").getOrCreate()

# Sample DataFrame
data = [("John", 25),
        ("Alice", 30),
        ("Bob", 28),
        ("Eve", 22),
        ("Mike", 35)]

columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)

# List of values to exclude
excluded_values_list = [22, 30]

# Filtering out rows with excluded values
filtered_df = df.where(~col("Age").isin(excluded_values_list))

# Show the result
filtered_df.show()
