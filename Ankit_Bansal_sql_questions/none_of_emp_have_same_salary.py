#from pyspark import F
from pyspark.sql import SparkSession
import findspark
from pyspark.sql.functions import col, sum, min, max, avg, count, countDistinct, desc

findspark.add_packages('org.postgresql:postgresql:42.2.22')
from sys import stdin


if __name__ == '__main__':

    spark = SparkSession.builder.appName("none_of_the_emp_have_same_salary").getOrCreate()

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
    # sql query :   select d.dep_name
                    # from employee e
                    # join dept d on e.dept_id = d.dep_id
                    # group by d.dep_name
                    # having count(e.emp_id) = count(distinct e.salary)

    df = emp_df.join(dept_df,emp_df["dept_id"] == dept_df["dep_id"],how="inner")

    grouped_df =  df.groupby(df["dep_name"])\
      .agg(count(df["emp_id"]).alias("no_emp_id") , countDistinct(df["salary"]).alias("no_distinct_salary"))

    filter_df = grouped_df.select(grouped_df["dep_name"])\
       .where(grouped_df["no_emp_id"] == grouped_df["no_distinct_salary"])




    # To check the Broadcast Join Threshold
    broadcast_threshold =spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    print('broadcast_threshold : ',broadcast_threshold)

    df.show(truncate=False)
    grouped_df.show(truncate=False)
    filter_df.show(truncate=False)
    #print(df.count())
    #print("result : ",result)

    stdin.readline()


