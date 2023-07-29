from pyspark.sql import SparkSession
import findspark
findspark.add_packages('org.postgresql:postgresql:42.2.22')
import sys


if __name__ == '__main__':
    # Configure SparkSession
    spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
    #tableName = sys.argv[0]

    jdbcDF = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='activity_511',
        #dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()
    #jdbcDF.printSchema()
    jdbcDF.show()




