from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

# Create Spark session
spark = SparkSession.builder.appName("thebigdatashow.me").getOrCreate()

# Create Spark DataFrames using Product Data
data = [
    (1, "Smartphone"),
    (2, "Tablet"),
    (3, "Laptop"),
    (4, "Monitor")
]

columns = ["id", "product_name"]

product_df = spark.createDataFrame(data, columns) # create product DataFrame


# Create Spark DataFrame using Sales Data

sales_df = spark.createDataFrame([
    (1, 250),
    (1, 300),
    (2, 200),
    (3, 450),
    (4, 150),
    (3, 550),
    (2, 220)
], ["product_id", "amount"])


# Let's assume that product_df is very small w.r.t sales_df

# Perform a broadcast join by broadcasting the smaller DF i.e. product_df
boardcasted_product_df = broadcast(product_df)

joined_df = sales_df.join(
    boardcasted_product_df,
    sales_df.product_id == boardcasted_product_df.id
)


joined_df.show()

joined_df.explain()