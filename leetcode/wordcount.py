from pyspark import SparkContext
import findspark
from sys import stdin

if __name__ == '__main__':

    sc = SparkContext("local","wordcountproblem")
    input = sc.textFile("C:/Users/Windows 10/PycharmProjects/learnSpark/Dataset/file.txt")

    rdd1 = input.flatMap(lambda x : x.split(" "))
    rdd = rdd1.map(lambda x : x.lower())
    rdd2 = rdd.map(lambda x : (x,1))
    rdd3 = rdd2.reduceByKey(lambda x,y : x+y)
    print("$$$$$$$$")
    # Print the execution plan
    print(rdd3.toDebugString())
    print("%%%%%%%%%%%%%%%%")
    result = rdd3.collect()
    result2 = rdd3.count()

    for i in result:
        print(i)
    print("result2 ", result2)
    stdin.readline()

