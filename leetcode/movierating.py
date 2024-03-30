from pyspark import SparkContext
from sys import stdin

if __name__ == '__main__':

    sc = SparkContext("local","movierating")
    input = sc.textFile("C:/Users/Windows 10/PycharmProjects/learnSpark/Dataset/movie_rating.txt")
    print(input.toDebugString())
    rdd = input.countByValue()
    ##print(rdd.toDebugString())   note: to debugstring humhe action se phele use karna hai.

    print(rdd)

    stdin.readline()

    #for i in rdd :
    #    print(i)
