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
    
    nycount = mapPartitions.filter(lambda x: "NY" in x[16] ).count()    
    othercount = mapPartitions.filter(lambda x: "NY" not in x[16] ).count()
    
    res = sc.parallelize([("NY",nycount),("Other",othercount)])
    res = res.map(lambda x:x[0]+"\t"+str(x[1]))
    res.saveAsTextFile("task4.out") 
    sc.stop()
