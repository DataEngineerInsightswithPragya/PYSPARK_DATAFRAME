#from pyspark import F
from pyspark.sql import SparkSession
import findspark
from pyspark.sql.functions import col, sum, min, max, avg

findspark.add_packages('org.postgresql:postgresql:42.2.22')
from sys import stdin


if __name__ == '__main__':

    spark = SparkSession.builder.appName("student_marks").getOrCreate()

    print("spark  : ",spark)

    order_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='exams',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    order_df.printSchema()
    # sql query :   select student_id,marks,count(*) from exams
                    # where subject in ('Chemistry','Physics')
                    # group by student_id,marks
                    # having count(*) = 2

    df = order_df.where(order_df['subject'].isin('Chemistry','Physics'))\
                        .groupby(order_df['student_id'],order_df["marks"]).count()

    df = df.where(df['count'] == 2 )




    df.show()
    #print(df.count())
    #print("result : ",result)

    stdin.readline()


