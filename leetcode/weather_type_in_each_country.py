from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField,IntegerType,StringType,DateType
from pyspark.sql.functions import col,min,max,avg,when


spark = SparkSession.builder.appName("testing").getOrCreate()


# Define schema for countries table
countries_schema = StructType([
    StructField("country_id", IntegerType(), True),
    StructField("country_name", StringType(), True)
])

# Create data for countries table
countries_data = [
    (2, "USA"),
    (3, "Australia"),
    (7, "Peru"),
    (5, "China"),
    (8, "Morocco"),
    (9, "Spain")
]

# Create DataFrame for countries table
countries_df = spark.createDataFrame(countries_data, schema=countries_schema)

# Define schema for weather table
weather_schema = StructType([
    StructField("country_id", IntegerType(), True),
    StructField("weather_state", IntegerType(), True),
    StructField("day", StringType(), True)
])

# Create data for weather table
weather_data = [
    (2, 15, "2019-11-01"),
    (2, 12, "2019-10-28"),
    (2, 12, "2019-10-27"),
    (3, -2, "2019-11-10"),
    (3, 0, "2019-11-11"),
    (3, 3, "2019-11-12"),
    (5, 16, "2019-11-07"),
    (5, 18, "2019-11-09"),
    (5, 21, "2019-11-23"),
    (7, 25, "2019-11-28"),
    (7, 22, "2019-12-01"),
    (7, 20, "2019-12-02"),
    (8, 25, "2019-11-05"),
    (8, 27, "2019-11-15"),
    (8, 31, "2019-11-25"),
    (9, 7, "2019-10-23"),
    (9, 3, "2019-12-23")
]

# Create DataFrame for weather table
weather_df = spark.createDataFrame(weather_data, schema=weather_schema)

# Show the DataFrames
countries_df.show()
weather_df.show()



# sql query : select
#     country_name,
#     case
#         when avg(weather_state) <= 15 then "Cold"
#         when avg(weather_state) >= 25 then "Hot"
#         else "Warm"
#     end as weather_type
# from countries join weather on countries.country_id = weather.country_id
# where weather.day >= "2019-11-01" and  weather.day <= "2019-11-30"
# group by countries.country_id



joined_df = countries_df.join(weather_df, countries_df["country_id"] == weather_df["country_id"],"inner")\
                        .where((weather_df["day"] >= "2019-11-01") & (weather_df["day"] <= "2019-11-30"))\
                        .drop(weather_df["country_id"])


joined_df.show()

grouped_df = joined_df.groupBy("country_name")\
                      .agg(avg("weather_state").alias("avg_temp"))

grouped_df.show()

final_df = grouped_df.withColumn("weather_type", when(grouped_df["avg_temp"] <= 15, "Cold") \
                                 .when(grouped_df["avg_temp"] >= 25, "Hot") \
                                 .otherwise("Warm"))

final_df.show()