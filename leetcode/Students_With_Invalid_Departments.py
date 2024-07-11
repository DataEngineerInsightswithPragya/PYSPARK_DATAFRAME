from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("pyspark").getOrCreate()

schema1 = StructType(
    [
        StructField("id",IntegerType(),True),
        StructField("name",StringType(),True)
    ]
)

departments_data = [
    (1, "Electrical Engineering"),
    (7, "Computer Engineering"),
    (13, "Business Administration")
]


department_df = spark.createDataFrame(data = departments_data , schema = schema1)
department_df.show(truncate = False)


# Students table schema
students_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department_id", IntegerType(), True)
])

# Students table data
students_data = [
    (23, "Alice", 1),
    (1, "Bob", 7),
    (5, "Jennifer", 13),
    (2, "John", 14),
    (4, "Jasmine", 77),
    (3, "Steve", 74),
    (6, "Luis", 1),
    (8, "Jonathan", 7),
    (7, "Daiana", 33),
    (11, "Madelynn", 1)
]

# Create Students DataFrame
student_df = spark.createDataFrame(students_data, schema=students_schema)
student_df.show(truncate = False)


#sql query
# select
#     s.id,
#     s.name
# from  students s left join departments d
# on s.department_id = d.id
# where s.department_id not in (select id from departments)


joined_df = student_df.join(department_df, department_df["id"] == student_df["department_id"], "left") \
    .select(student_df["id"], student_df["name"], department_df["id"].alias("dept_id"))

joined_df.show()

final_df = joined_df.select("id", "name").where(col("dept_id").isNull())

final_df.show()