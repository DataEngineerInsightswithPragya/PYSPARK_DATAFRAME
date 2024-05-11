from Tools.scripts.dutree import display
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, split, regexp_extract

if __name__ == "__main__":
    spark = SparkSession.builder.appName("regex").getOrCreate()
    data = [(1, "Sagar-Prajapati"), (2, "Alex-John"), (3, "John Cena"), (4, "Kim Joe")]
    schema = "ID int,Name string"
    df = spark.createDataFrame(data, schema)
    df.show()

    df2 = df.select("*").withColumn("new",regexp_replace(col("Name"),"-",' '))

    #df2 = df.select("*").withColumn("new", regexp_replace(col("Name"), r"-", " "))
    df2.show()

    df3 = df2.select("ID","Name",split(col("new"),' ').alias("new"))
    df3.show()

    df4 = df3.select("*").withColumn("Firstname",df3["new"][0]).withColumn("lastname",df3["new"][1])
    df4.show()
    df4 = df4.drop('new')
    df4.show()

    df5 = df4.withColumn("Lastname",col("lastname"))
    df5.show()

    df6 = df5.select("*").withColumn("correct_name_format",regexp_extract(col("Name"),r"([a-zA-Z]* [a-zA-Z]*)",1))
    df6.show()
    #display(df6)


