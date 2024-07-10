from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, to_date , rank , count
from pyspark.sql.window import Window

# Create a Spark session
spark = SparkSession.builder.master("local[*]").appName("PySparkExample").getOrCreate()

# Define the schema
schema = StructType([
    StructField("username", StringType(), True),
    StructField("activity", StringType(), True),
    StructField("startDate", StringType(), True),
    StructField("endDate", StringType(), True)
])

# Create the data
data = [
    ("Alice", "Travel", "2020-02-12", "2020-02-20"),
    ("Alice", "Dancing", "2020-02-21", "2020-02-23"),
    ("Alice", "Travel", "2020-02-24", "2020-02-28"),
    ("Bob", "Travel", "2020-02-11", "2020-02-18")
]

# Create the DataFrame
df = spark.createDataFrame(data, schema)

# Convert the date strings to DateType
df = df.withColumn("startDate", to_date(col("startDate"), "yyyy-MM-dd"))
df = df.withColumn("endDate", to_date(col("endDate"), "yyyy-MM-dd"))

# Show the DataFrame
df.show()

#sql query : # Write your MySQL query statement below
# with cte as (
#     select
#         *,
#         rank() over(partition by username order by startdate desc ) as rnk,
#         count(username) over(partition by username) as cnt
#     from UserActivity

# )
# select username,
#     activity,
#     startDate,
#     endDate
# from cte where cnt = 1 or rnk = 2

windowSpec1 = Window.partitionBy(df["username"]).orderBy(df["startdate"].desc())
windowSpec2 = Window.partitionBy(df["username"])

intd_df = df.select("*")\
            .withColumn("rnk", rank().over(windowSpec1))\
            .withColumn("cnt",count(df["username"]).over(windowSpec2))
intd_df.show()


final_result_df = intd_df.select("username","activity","startdate","enddate")\
                         .where((col("cnt") == 1 ) | (col("rnk") == 2))

final_result_df.show()