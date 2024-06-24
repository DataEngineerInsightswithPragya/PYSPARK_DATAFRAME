#from pyspark import F
from pyspark.sql import SparkSession
import findspark
from pyspark.sql.functions import col, sum, min, max, avg, count, countDistinct, desc

findspark.add_packages('org.postgresql:postgresql:42.2.22')
from sys import stdin


if __name__ == '__main__':

    spark = SparkSession.builder.appName("dept_with_no_employee").getOrCreate()

    print("spark  : ",spark)

    employee_df_1 = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='employee',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    employee_df_2 = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='employee',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()




    employee_df_1.printSchema()

    #question : 1- write a query to print emp name , their manager name and diffrence in their age (in days)
                   #for employees whose year of birth is before their managers year of birth

    # sql query :   select e1.emp_name as emp_name,e2.emp_name as manager_name, e2.dob - e1.dob AS day_difference
                    # --DATEDIFF(day,e2.dob,e1.dob)
                    # from employee e1
                    # join employee e2
                    # on e1.manager_id = e2.emp_id
                    # where e1.dob < e2.dob

    # df = employee_df.alias("e1")\
    #     .join(
    #         employee_df.alias("e2"),
    #         employee_df["e1"]["manager_id"] == employee_df["e2"]["emp_id"],how= "inner")\
    #     .where(employee_df["e1"]["dob"] < employee_df["e2"]["dob"])

    df = employee_df_1.join(employee_df_2.alias("e2"),
                                      employee_df_1["manager_id"] == employee_df_2["emp_id"],
                                      how="inner") \
        .select(employee_df_1["emp_name"].alias("employee name"),employee_df_2["emp_name"].alias("manager name"),
                (employee_df_2["dob"] - employee_df_1["dob"]).alias("day_difference"))\
        .where(employee_df_1["dob"] < employee_df_2["dob"])







    # To check the Broadcast Join Threshold
    broadcast_threshold =spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    print('broadcast_threshold : ',broadcast_threshold)

    df.show(truncate=False)

    #print(df.count())
    #print("result : ",result)

    stdin.readline()


