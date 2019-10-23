from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    l = sc.textFile(sys.argv[1],1)
    mapPartitions = l.mapPartitions(lambda x:reader(x))
   
    l = mapPartitions.map(lambda x : (str(x[14])+", "+str(x[16]),1))
    res = l.reduceByKey(add).sortBy(lambda x: (-x[1],x[0])).take(20) 
    sc.parallelize(res).map(lambda xy: str(xy[0])+"\t"+str(xy[1])).saveAsTextFile("task6.out")
    sc.stop()
