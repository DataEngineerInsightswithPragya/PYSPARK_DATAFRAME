from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import col,sum,max

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Create DataFrame Example") \
    .getOrCreate()

# Define schema
schema = StructType([
    StructField("person_id", IntegerType(), True),
    StructField("person_name", StringType(), True),
    StructField("weight", IntegerType(), True),
    StructField("turn", IntegerType(), True)
])

# Create data
data = [
    (5, "Alice", 250, 1),
    (4, "Bob", 175, 5),
    (3, "Alex", 350, 2),
    (6, "John Cena", 400, 3),
    (1, "Winston", 500, 6),
    (2, "Marie", 200, 4)
]

# Create DataFrame
queue_df = spark.createDataFrame(data, schema)

# Show DataFrame
queue_df.show()

# sql query:
# with cte as (select
#     *,
#     sum(weight) over(order by turn) as running_weight
# from queue
# ) select person_name
#  from cte where running_weight = (select max(running_weight) from cte where running_weight <= 1000)

windowSpec = Window.orderBy("turn")

intd_df = queue_df.withColumn("running_weight", sum(col("weight")).over(windowSpec))
intd_df.show()

max_running_weight = intd_df.filter(col("running_weight") <= 1000).agg(max("running_weight")).collect()[0][0]

#  [Row(max(running_weight)=1000)]  .collect()
# Row(max(running_weight)=1000)  .collect()[0]
# max_running_weight :  1000  .collect()[0][0]

print("max_running_weight : ",max_running_weight)


# # Filter the DataFrame based on the calculated maximum running_weight
final_df = intd_df.filter(col("running_weight") == max_running_weight).select("person_name")

# # Show the result DataFrame
final_df.show()