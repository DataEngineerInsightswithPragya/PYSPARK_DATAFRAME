from pyspark.sql import SparkSession

from sys import stdin


def state_convert(code):
    return broadcastStates.value[code]


if __name__ == '__main__':

    spark = SparkSession.builder.appName('broadcast_using_df').getOrCreate()

    states = {"NY": "New York", "CA": "California", "FL": "Florida"}
    broadcastStates = spark.sparkContext.broadcast(states)

    data = [("James", "Smith", "USA", "CA"),
            ("Michael", "Rose", "USA", "NY"),
            ("Robert", "Williams", "USA", "CA"),
            ("Maria", "Jones", "USA", "FL")
            ]

    columns = ["firstname", "lastname", "country", "state"]
    df = spark.createDataFrame(data=data, schema=columns)
    df.printSchema()
    df.show(truncate=False)


    result = df.rdd.map(lambda x: (x[0], x[1], x[2], state_convert(x[3]))).toDF()

    # +-------+--------+---+----------+
    # | _1 | _2 | _3 | _4 |
    # +-------+--------+---+----------+
    # | James | Smith | USA | California |
    # | Michael | Rose | USA | New
    # York |
    # | Robert | Williams | USA | California |
    # | Maria | Jones | USA | Florida |

    result = df.rdd.map(lambda x: (x[0], x[1], x[2], state_convert(x[3]))).toDF(columns)
    result.show(truncate=False)


    #stdin.readline()

