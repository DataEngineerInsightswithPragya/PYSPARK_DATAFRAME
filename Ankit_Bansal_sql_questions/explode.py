from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col

# Create Spark session
spark = SparkSession.builder.appName("thebigdatashow.me").getOrCreate()

# Create a DataFrame
data = [
    ("Anku", ["Apple", "Banana", "Cherry"]),
    ("Sally", ["Kiwi", "Orange"]),
    ("Sada", ["Papaya", "Grapefruit", "Banana"])
]
columns = ["name", "fruits"]
df = spark.createDataFrame(data, columns)
df.show()

# Use explode. Explode is same as flatmap in rdd
df_exploded = df.withColumn("fruit", explode(col("fruits")))

df_exploded.show()

result = df_exploded.filter(col("fruit") == "Banana")

result.show()