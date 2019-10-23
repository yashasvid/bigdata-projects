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
    
    l_sum = mapPartitions.map(lambda x: (x[2],x[12]))
    l_sum = l_sum.reduceByKey(lambda x,y: float(x)+float(y))

    l_count = mapPartitions.map(lambda x:(x[2],1))
    l_count = l_count.reduceByKey(add) 
    r = l_sum.join(l_count)
    r = r.map(lambda x:  (x[0],(x[1][0],float(x[1][0])/int(x[1][1]))) ) 
    r = r.map(lambda x: str(x[0]) + "\t{0:.2f}".format(float(x[1][0]))  + ", {0:.2f}".format(float(x[1][1])))   
    r.saveAsTextFile("task3.out")
    sc.stop()
