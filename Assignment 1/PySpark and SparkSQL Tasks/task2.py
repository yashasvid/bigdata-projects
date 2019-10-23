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
    l = l.mapPartitions(lambda x:reader(x))
    l = l.map(lambda x: (x[2],1))
    
    res = l.reduceByKey(add)
    formatted_res = res.map(lambda xy: str(xy[0])+"\t"+str(xy[1]))   
    formatted_res.saveAsTextFile("task2.out")
   
    sc.stop()
