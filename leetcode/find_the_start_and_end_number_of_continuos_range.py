from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Initialize Spark session
spark = SparkSession.builder \
    .appName("LogIDData") \
    .getOrCreate()

# Define schema for log_id table
log_id_schema = StructType([
    StructField("log_id", IntegerType(), True)
])

# Create data for log_id table
log_id_data = [
    (1,),
    (2,),
    (3,),
    (7,),
    (8,),
    (10,)
]

# Create DataFrame for log_id table
log_id_df = spark.createDataFrame(log_id_data, schema=log_id_schema)

# Show the DataFrame
log_id_df.show()

log_id_df.printSchema()

# sql query :with cte as (select
#     log_id,
#     row_number() over(order by log_id) as rownumber
# from logs
# ) select min(log_id) as start_id, max(log_id) as end_id from cte
# group by (log_id - rownumber)


windowSpec = Window.orderBy(log_id_df["log_id"])

df = log_id_df.withColumn("rownumber", row_number().over(windowSpec))
df.show()

diff_df = df.withColumn("diff",(df["log_id"] - df["rownumber"]))
diff_df.show()

final_result_df = diff_df.groupBy("diff").agg(min(diff_df["log_id"]).alias("start_id"),max(diff_df["log_id"]).alias("end_id"))

final_result_df.show()