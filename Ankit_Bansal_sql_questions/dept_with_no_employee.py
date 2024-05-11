#from pyspark import F
from pyspark.sql import SparkSession
import findspark
from pyspark.sql.functions import col, sum, min, max, avg, count, countDistinct, desc

findspark.add_packages('org.postgresql:postgresql:42.2.22')
from sys import stdin


if __name__ == '__main__':

    spark = SparkSession.builder.appName("dept_with_no_employee").getOrCreate()

    print("spark  : ",spark)

    employee_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='employee',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()


    dept_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='dept',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    employee_df.printSchema()
    dept_df.printSchema()
    # sql query :   select d.dep_name
                    # from dept d
                    # left join employee e
                    # on e.dept_id = d.dep_id
                    # where e.dept_id is null

    df = dept_df.join(employee_df, dept_df["dep_id"] == employee_df["dept_id"],how= "left")\
        .where(employee_df["dept_id"].isNull())





    # To check the Broadcast Join Threshold
    broadcast_threshold =spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    print('broadcast_threshold : ',broadcast_threshold)

    df.show(truncate=False)

    #print(df.count())
    #print("result : ",result)

    stdin.readline()


