from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import to_date , col,sum
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Create DataFrame Example") \
    .getOrCreate()

# Define schema
schema = StructType([
    StructField("player_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("day", StringType(), True),
    StructField("score_points", IntegerType(), True)
])

# Create data
data = [
    ("Aron", "F", "2020-01-01", 17),
    ("Alice", "F", "2020-01-07", 23),
    ("Bajrang", "M", "2020-01-07", 7),
    ("Khali", "M", "2019-12-25", 11),
    ("Slaman", "M", "2019-12-30", 13),
    ("Joe", "M", "2019-12-31", 3),
    ("Jose", "M", "2019-12-18", 2),
    ("Priya", "F", "2019-12-31", 23),
    ("Priyanka", "F", "2019-12-30", 17)
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

df.printSchema()

# Show DataFrame
df.show()

df = df.withColumn("day",to_date(col("day"),"yyyy-MM-dd"))
df.printSchema()
df.show()


# #sql query
# select
#     gender,day,
# sum(score_points) over(partition by gender order by day RANGE between UNBOUNDED PRECEDING and CURRENT ROW) as total
# from scores
# order by gender,day

windowSpec = Window.partitionBy("gender").orderBy("day").rangeBetween(Window.unboundedPreceding , Window.currentRow)

df_with_total = df.withColumn("total", sum(df["score_points"]).over(windowSpec))

df_with_total.show()

final_df = df_with_total.select("gender","day","total").orderBy("gender","day")

final_df.show()