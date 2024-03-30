from pyspark import SparkContext
from sys import stdin

if __name__ == '__main__':

    sc = SparkContext("local",'campaign')
    input = sc.textFile("C:/Users/Windows 10/PycharmProjects/learnSpark/Dataset/campaign.csv")

    rdd1  = input.map(lambda x : (x.split(",")[0],float(x.split(",")[2])))
    rdd2 = rdd1.map(lambda x : (x[1],x[0]))
    rdd3 = rdd2.flatMapValues(lambda x : x.split(" "))
    rdd4 = rdd3.map(lambda x: (x[1], x[0]))
    rdd5 = rdd4.reduceByKey(lambda x,y : x+y)
    result = rdd5.collect()

    for i in result:
        print(i)

    stdin.readline()


