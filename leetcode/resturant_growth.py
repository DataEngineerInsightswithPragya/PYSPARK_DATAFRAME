from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg as _avg, count as _count, round as _round
from pyspark.sql.window import Window
from pyspark.sql.types import StructField, StructType,IntegerType,StringType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("MySQL to PySpark Conversion") \
    .getOrCreate()
# Define schema
schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("visited_on", StringType(), True),
    StructField("amount", IntegerType(), True)
])

# Create data
data = [
    (1, "Jhon", "2019-01-01", 100),
    (2, "Daniel", "2019-01-02", 110),
    (3, "Jade", "2019-01-03", 120),
    (4, "Khaled", "2019-01-04", 130),
    (5, "Winston", "2019-01-05", 110),
    (6, "Elvis", "2019-01-06", 140),
    (7, "Anna", "2019-01-07", 150),
    (8, "Maria", "2019-01-08", 80),
    (9, "Jaze", "2019-01-09", 110),
    (1, "Jhon", "2019-01-10", 130),
    (3, "Jade", "2019-01-10", 150)
]

# Create DataFrame
customer_df = spark.createDataFrame(data, schema)

# Step 1: Create CTE1 with grouped sums
cte1_df = customer_df.groupBy("visited_on").agg(_sum("amount").alias("amount"))

cte1_df.show()

# Define window specification for the last 7 days
window_spec = Window.orderBy("visited_on").rowsBetween(-6, 0)

# Step 2: Create CTE2 with running sums, averages, and counts
cte2_df = cte1_df.withColumn("amount_sum", _sum("amount").over(window_spec)) \
    .withColumn("avg_tot", _avg("amount").over(window_spec)) \
    .withColumn("cnt", _count("visited_on").over(window_spec))

cte2_df.show()

# Step 3: Select the required columns and filter by count
final_df = cte2_df.filter(col("cnt") == 7) \
    .select("visited_on", "amount_sum", _round("avg_tot", 2).alias("average_amount"))

# Show the result DataFrame
final_df.show()



# sql query:
# WITH CTE1 as (select visited_on, sum(amount) as amount from customer group by visited_on)
# , cte2 as (
#     select
#     visited_on,
#         sum(amount) over(order by visited_on ROWS between 6 PRECEDING and current row) as amount ,
#    avg(amount) over(order by visited_on ROWS between 6 PRECEDING and current row) as avg_tot,
#      COUNT(1) OVER (ORDER BY VISITED_ON ROWS between 6 PRECEDING and current row) as cnt
#  from cte1
#  order by visited_on
#  )
#  Select visited_on , amount , round(avg_tot,2) as average_amount from cte2 where cnt = 7