from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,IntegerType,StructType,StructField
from pyspark.sql.functions import col, to_date , sum


spark = SparkSession.builder.appName("problem").getOrCreate()

schema1 = StructType(
    [
        StructField("product_id",IntegerType(),True),
        StructField("product_name",StringType(),True),
        StructField("product_category",StringType(),True)
    ]
)

products_data = [
    (1, "Leetcode Solutions", "Book"),
    (2, "Jewels of Stringology", "Book"),
    (3, "HP", "Laptop"),
    (4, "Lenovo", "Laptop"),
    (5, "Leetcode Kit", "T-shirt")
]

schema2 = StructType(
    [
        StructField("product_id",IntegerType(),True),
        StructField("order_date",StringType(),True),
        StructField("unit",IntegerType(),True)
    ]
)

orders_data = [
    (1, "2020-02-05", 60),
    (1, "2020-02-10", 70),
    (2, "2020-01-18", 30),
    (2, "2020-02-11", 80),
    (3, "2020-02-17", 2),
    (3, "2020-02-24", 3),
    (4, "2020-03-01", 20),
    (4, "2020-03-04", 30),
    (4, "2020-03-04", 60),
    (5, "2020-02-25", 50),
    (5, "2020-02-27", 50),
    (5, "2020-03-01", 50)
]


product_df = spark.createDataFrame(data =products_data, schema = schema1 )

order_df = spark.createDataFrame(data =orders_data, schema = schema2 )

product_df.show()
order_df.show()

product_df.printSchema()
order_df.printSchema()


order_df = order_df.withColumn("order_date",to_date(col("order_date"),"yyyy-MM-dd"))

order_df.show()
order_df.printSchema()


#sql query :

# select
#     p.product_name,
#     sum(o.unit) as unit
# from products p join orders o
# on p.product_id = o.product_id
# where o.order_date >= "2020-02-01" and o.order_date <= "2020-02-29"
# group by p.product_name
# having unit >= 100


joined_df = product_df.join(order_df,product_df["product_id"] == order_df["product_id"],"inner")\
                       .where((order_df["order_date"] >= "2020-02-01") & (order_df["order_date"] <= "2020-02-29"))\
                      .groupBy(col("product_name")).agg(sum(col("unit")).alias("unit"))



joined_df.show()

final_df = joined_df.select("product_name","unit").where(col("unit") >= 100)
final_df.show()