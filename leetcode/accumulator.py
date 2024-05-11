from pyspark import SparkContext

from sys import stdin

# def count_lines(line):
#     if (line == " "):
#         acc_count.add(1)

if __name__ == '__main__':
    sc = SparkContext("local[*]", "accumulator")
    #print(sc.defaultParallelism)
    #print(sc.defaultMinPartitions)

    #list1 = list[1,2,3,4,5,7,9]

    #rdd7 = sc.parallelize([1,2,3,4,5,6,7,8,9,10])
    #print(sc.defaultParallelism)
    #print(rdd7.getNumPartitions())


    rdd1 = sc.textFile("C:/Users/Windows 10/PycharmProjects/learnSpark/Dataset/spaces_count.txt")
    #print(rdd1.getNumPartitions())
    acc_count = sc.accumulator(0)




    def count_lines(line):
        if (len(line) == 0):
            acc_count.add(1)


    rdd2= rdd1.repartition(5)
    print(rdd2.getNumPartitions())

    rdd2.foreach(count_lines)
    print(acc_count.value)

    stdin.readline()
