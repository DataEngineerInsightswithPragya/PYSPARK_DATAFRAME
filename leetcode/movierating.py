from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import to_date,col,count,avg

# Create a Spark session
spark = SparkSession.builder \
    .appName("Create DataFrames") \
    .getOrCreate()

# Define schema for Movies table
movies_schema = StructType([
    StructField("movie_id", IntegerType(), True),
    StructField("title", StringType(), True)
])

# Define data for Movies table
movies_data = [
    (1, "Avengers"),
    (2, "Frozen 2"),
    (3, "Joker")
]

# Create DataFrame for Movies table
movies_df = spark.createDataFrame(movies_data, schema=movies_schema)
movies_df.show()

# Define schema for Users table
users_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("name", StringType(), True)
])

# Define data for Users table
users_data = [
    (1, "Daniel"),
    (2, "Monica"),
    (3, "Maria"),
    (4, "James")
]

# Create DataFrame for Users table
users_df = spark.createDataFrame(users_data, schema=users_schema)
users_df.show()

# Define schema for MovieRating table
movie_rating_schema = StructType([
    StructField("movie_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("created_at", StringType(), True)  # Keeping it as StringType to simplify
])

# Define data for MovieRating table
movie_rating_data = [
    (1, 1, 3, "2020-01-12"),
    (1, 2, 4, "2020-02-11"),
    (1, 3, 2, "2020-02-12"),
    (1, 4, 1, "2020-01-01"),
    (2, 1, 5, "2020-02-17"),
    (2, 2, 2, "2020-02-01"),
    (2, 3, 2, "2020-03-01"),
    (3, 1, 3, "2020-02-22"),
    (3, 2, 4, "2020-02-25")
]

# Create DataFrame for MovieRating table
movie_rating_df = spark.createDataFrame(movie_rating_data, schema=movie_rating_schema)
movie_rating_df.show()



movie_rating_df = movie_rating_df.withColumn("created_at",to_date(col("created_at"),"yyyy-MM-dd"))
movie_rating_df.show()
movie_rating_df.printSchema()

# #sql query

# (select
#     name as results
# from users u join movierating m
# on u.user_id = m.user_id
# group by m.user_id
# order by count(m.user_id)  desc , name
# limit 1)


# union all

# (select
#     title as name
# from movies mv join movierating m
# on mv.movie_id = m.movie_id
# where m.created_at >= '2020-02-01' and m.created_at <= '2020-02-29'
# group by m.movie_id
# order by avg(rating) desc,name
# limit 1
# )

# movie_rating_df
# users_df
# movies_df


joined_df1 = users_df.join(movie_rating_df,users_df["user_id"] == movie_rating_df["user_id"])\
                     .groupBy(users_df["name"]).agg(count(movie_rating_df["user_id"]).alias("cnt"))\
                     .orderBy(col("cnt").desc(),col("name")).limit(1)
joined_df1.show()

joined_df1 = joined_df1.select("name")


joined_df2 = movies_df.join(movie_rating_df,movies_df["movie_id"] == movie_rating_df["movie_id"])\
                     .where((movie_rating_df["created_at"] >= "2020-02-01") & (movie_rating_df["created_at"] <= "2020-02-29") )\
                     .groupBy(movies_df["title"]).agg(avg(movie_rating_df["rating"]).alias("avg"))\
                     .orderBy(col("avg").desc(),col("title")).limit(1)
joined_df2.show()
joined_df2 = joined_df2.select(col("title").alias("name"))


union_all_df = joined_df1.unionAll(joined_df2)
union_all_df.show()