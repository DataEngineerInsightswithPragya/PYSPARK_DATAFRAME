#from pyspark import F
from pyspark.sql import SparkSession
import findspark
from pyspark.sql.functions import col, sum, min, max, avg, count, countDistinct, desc

findspark.add_packages('org.postgresql:postgresql:42.2.22')
from sys import stdin


if __name__ == '__main__':

    spark = SparkSession.builder.appName("return_order").getOrCreate()

    print("spark  : ",spark)

    emp_df = spark.read.format("jdbc"). \
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

    emp_df.printSchema()
    dept_df.printSchema()
    # sql query :   select d.dep_name,avg(e.salary)
                    # from employee e join dept d
                    # on e.dept_id = d.dep_id
                    # group by d.dep_name

    df = emp_df.join(dept_df,emp_df['dept_id'] == dept_df['dep_id'],how='inner')\
        .groupby(dept_df['dep_name'])\
        .agg(avg(emp_df['salary']).alias("avg_salary"))


    # To check the Broadcast Join Threshold
    broadcast_threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    print('broadcast_threshold : ',broadcast_threshold)

    df.show(truncate=False)
    #print(df.count())
    #print("result : ",result)

    stdin.readline()


