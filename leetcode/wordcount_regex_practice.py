from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, explode, split, lower
from pyspark.sql.types import StructType, StructField , StringType

spark = SparkSession.builder.appName("WordCount").master("local[*]").getOrCreate()

class WordCount:
    def createDF(self):
        line = """In a world full of #technology, understanding data is key to success.Data analytics, 'AI', and machine learning are transforming industries.Companies are racing to harness the power of data-driven insights;But,data is messy and comes in various formats - structured,unstructured,and semi-structured.The challenge is to clean, process, and  analyze this data effectively.There's a growing demand for data scientists, analysts, and engineers who can unlock the value hidden within the data."""

        schema = StructType([StructField("text",StringType(),True)])
        # Convert text data into a list of tuples
        word_data = [(line,)]
        print("data : ", word_data)

        textdf = spark.createDataFrame(data = word_data,schema = schema)
        #dataDf = spark.createDataFrame([(line,)], ['text'])

        textdf.show(truncate=False)
        return textdf

    def wordcount(self,inputdf):
        pattern = "[#.,'-;]"
        replace_with = " "
        regex_df = inputdf.select(regexp_replace(col("text"),pattern,replace_with).alias("words"))
        regex_df.show(truncate= False)
        splitdf = regex_df.select(split(lower("words")," ").alias("words"))
        #splitDf = regex_df.select(explode(split(lower('words'), ' ')).alias('words'))
        splitdf.show(truncate=False)

        explodedf = splitdf.select(explode(col("words")).alias("words"))
        explodedf.show(truncate=False)

        countdf = explodedf.groupby("words").count().alias("words").filter(col('words') != '').orderBy('words')
        countdf.show(50)






if __name__ == "__main__" :
    ob = WordCount()
    inputdf = ob.createDF()
    resultdf = ob.wordcount(inputdf)