from pyspark.sql import SparkSession
import findspark
findspark.add_packages('org.postgresql:postgresql:42.2.22')
from sys import stdin


if __name__ == '__main__':

    spark = SparkSession.builder.appName("find_customer_name").getOrCreate()

    print("spark  : ",spark)

    order_df = spark.read.format("jdbc"). \
        options(
        url='jdbc:postgresql://localhost:5432/sample_db',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='orders',
        # dbtable= tableName,
        user='postgres',
        password='Pragya',
        driver='org.postgresql.Driver'). \
        load()

    order_df.printSchema()

    ## To test the number of task generated
    #print(spark.sql.shuffle.partitions)
    #spark.conf.set("spark.sql.shuffle.partitions", "your_desired_number_of_partitions")
    #order_df.show()
    #rdd1 = order_df.rdd
    #print(rdd1.getNumPartitions())
    #rdd2 = rdd1.repartition(5)
    #print(rdd2.getNumPartitions())
    #order_df2 = rdd2.toDF()

    #rdd2.collect()


    # sql query : select * from orders where customer_name like '_a_d%'

    df2 = order_df.select("*").where(order_df["customer_name"].like('_a_d%'))
    rdd = df2.rdd
    print(rdd.toDebugString())
    df2.show()

    stdin.readline()
    #spark.stop()
