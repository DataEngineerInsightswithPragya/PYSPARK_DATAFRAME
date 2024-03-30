from pyspark import SparkContext

from sys import stdin

def loadboringwords():
    setlist = set()
    lines = open("C:/Users/Windows 10/PycharmProjects/learnSpark/Dataset/boringwords.txt").readlines()
    print('lines : ',lines)

    for i in lines:
        setlist.add(i.strip()) # Strip any leading/trailing whitespaces and add the word to the set
    print("setlist : ", setlist)
    return setlist


if __name__ == '__main__':

    sc = SparkContext("local",'broadcast')

    brod_variable = sc.broadcast(loadboringwords())

    input = sc.textFile("C:/Users/Windows 10/PycharmProjects/learnSpark/Dataset/file.txt")

    rdd1 = input.flatMap(lambda x : x.split(" "))

    rdd2 = rdd1.filter(lambda x : x not in brod_variable.value)

    rdd3 = rdd2.map(lambda x : (x,1))
    rdd4 = rdd3.reduceByKey(lambda x,y : x+y)
    result = rdd4.collect()

    for i in result:
        print(i)

    stdin.readline()

