from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import sum,count

# Initialize Spark session
spark = SparkSession.builder \
    .appName("EducationData") \
    .getOrCreate()

# Define schema for Students table
students_schema = StructType([
    StructField("student_id", IntegerType(), True),
    StructField("student_name", StringType(), True)
])

# Create data for Students table
students_data = [
    (1, "Alice"),
    (2, "Bob"),
    (13, "John"),
    (6, "Alex")
]

# Create DataFrame for Students table
students_df = spark.createDataFrame(students_data, schema=students_schema)

# Define schema for Subjects table
subjects_schema = StructType([
    StructField("subject_name", StringType(), True)
])

# Create data for Subjects table
subjects_data = [
    ("Math",),
    ("Physics",),
    ("Programming",)
]

# Create DataFrame for Subjects table
subjects_df = spark.createDataFrame(subjects_data, schema=subjects_schema)

# Define schema for Examinations table
examinations_schema = StructType([
    StructField("student_id", IntegerType(), True),
    StructField("subject_name", StringType(), True)
])

# Create data for Examinations table
examinations_data = [
    (1, "Math"),
    (1, "Physics"),
    (1, "Programming"),
    (2, "Programming"),
    (1, "Physics"),
    (1, "Math"),
    (13, "Math"),
    (13, "Programming"),
    (13, "Physics"),
    (2, "Math"),
    (1, "Math")
]

# Create DataFrame for Examinations table
examinations_df = spark.createDataFrame(examinations_data, schema=examinations_schema)

# Show the DataFrames
students_df.show()
subjects_df.show()
examinations_df.show()


# sql query :   select
#     s.student_id,
#     s.student_name,
#     sub.subject_name,
#     count(e.subject_name) AS attended_exams
# from students s cross join subjects sub left join examinations e on s.student_id = e.student_id and sub.subject_name = e.subject_name
# group by s.student_id,sub.subject_name
# order by s.student_id,sub.subject_name


joined_df = students_df.join(subjects_df)\
                        .join(examinations_df , (students_df["student_id"] == examinations_df["student_id"])
                        & (subjects_df["subject_name"] == examinations_df["subject_name"]),"left")\
                        .drop(examinations_df["student_id"])\
                        .drop(subjects_df["subject_name"])\
                        .orderBy("student_id")

joined_df.show()

windowSpec = Window.partitionBy("student_id","subject_name")

final_df = joined_df.withColumn("attended_exams",count("subject_name").over(windowSpec))

final_df = final_df.orderBy("student_id","subject_name")


final_df.show()