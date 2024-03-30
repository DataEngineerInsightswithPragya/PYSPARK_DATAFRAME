from pyspark import SparkContext
from sys import stdin

if __name__ == '__main__':

    sc = SparkContext("local","customer")
    input = sc.textFile("C:/Users/Windows 10/PycharmProjects/learnSpark/Dataset/customer.csv")

    rdd1 = input.map(lambda x : (x.split(",")[0],float(x.split(",")[2])))

    rdd2 = rdd1.reduceByKey(lambda x,y : x+y)

    rdd3 = rdd2.sortBy(lambda x : x[1],False)

    print(rdd3.toDebugString())

    result = rdd3.collect()

    for i in result:
        print(i)

    stdin.readline()
