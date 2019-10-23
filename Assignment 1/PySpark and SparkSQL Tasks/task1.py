from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    l = sc.textFile(sys.argv[1],1)
    l = l.mapPartitions(lambda x:reader(x))
    l = l.map(lambda x: (x[0], x[14]+", "+ x[6]+ ", "+ x[2]+ ", "+ x[1]))
    
    o_lines = sc.textFile(sys.argv[2],1)
    o_lines = o_lines.mapPartitions(lambda x:reader(x))
    o_lines = o_lines.map(lambda x: (x[0],"open"))
    
    res = l.subtractByKey(o_lines)
    res = res.map(lambda xy: str(xy[0]+"\t"+xy[1]))
    res.saveAsTextFile("task1.out")
   
    sc.stop()
