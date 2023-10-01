from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# Create a Spark session
spark = SparkSession.builder.appName("SchemaDataFrame").getOrCreate()

# Define the schema
schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("Date", DateType(), True),
    StructField("Amount", IntegerType(), True),
    StructField("Status", StringType(), True)
])

# Sample data
data = [
    (1, "2013-07-25", 1159, "closed"),
    (2, "2014-07-25", 256, "pending_payment"),
    (3, "2013-07-25", 1159, "complete"),
    (4, "2019-07-25", 1159, "closed")
]

# Create the DataFrame with the specified schema
df = spark.createDataFrame(data, schema=schema)

# Convert the "Date" column to Unix timestamp and create a new column
df = df.withColumn("UnixTimestamp", unix_timestamp(df["Date"], "yyyy-MM-dd").cast("long"))

# Show the DataFrame with the new column
df.show()
