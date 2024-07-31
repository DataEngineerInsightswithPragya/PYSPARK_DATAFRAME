from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("CreateDataFrameExample") \
    .getOrCreate()

# Define the data for the Friendship table
friendship_data = [
    (1, 2),
    (1, 3),
    (1, 4),
    (2, 3),
    (2, 4),
    (2, 5),
    (6, 1)
]

# Define the schema for the Friendship table
friendship_schema = ["user1_id", "user2_id"]

# Create a DataFrame for the Friendship table
friendship_df = spark.createDataFrame(friendship_data, schema=friendship_schema)

# Show the Friendship DataFrame
friendship_df.show()

# Define the data for the Likes table
likes_data = [
    (1, 88),
    (2, 23),
    (3, 24),
    (4, 56),
    (5, 11),
    (6, 33),
    (2, 77),
    (3, 77),
    (6, 88)
]

# Define the schema for the Likes table
likes_schema = ["user_id", "page_id"]

# Create a DataFrame for the Likes table
likes_df = spark.createDataFrame(likes_data, schema=likes_schema)

# Show the Likes DataFrame
likes_df.show()


# sql query:  # Write your MySQL query statement below
# WITH CTE AS
# (SELECT
# CASE
# WHEN user1_id = 1 THEN user2_id
# WHEN user2_id = 1 THEN user1_id
# END AS friend_id FROM Friendship
# ),cte2 as (select
#  friend_id,
#  page_id
# from cte c join likes l
# on c.friend_id = l.user_id)
# select distinct page_id as recommended_page from cte2 where page_id not in ( select page_id from likes where user_id = 1)



friend_df = friendship_df.withColumn(
    "friend_id",
    when(col("user1_id") == 1, col("user2_id"))
    .when(col("user2_id") == 1, col("user1_id"))
)

# Show the new DataFrame
friend_df.show()


joined_df = friend_df.join(likes_df,friend_df["friend_id"] == likes_df["user_id"],"inner")\
                    .select("friend_id","page_id")

joined_df.show()

user1_df = likes_df.select("page_id").where(col("user_id") == 1).collect()[0][0]

print(user1_df)

# user1_pages = likes_df.filter(col("user_id") == 1).select("page_id").rdd.flatMap(lambda x: x).collect()

# print(user1_pages)


final_df = joined_df.select("page_id").distinct()\
                    .where(~joined_df["page_id"].isin(user1_df))

final_df.show()

