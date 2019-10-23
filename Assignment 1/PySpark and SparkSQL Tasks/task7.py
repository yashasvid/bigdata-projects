from __future__ import print_function
import os
import string
from decimal import Decimal
import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
	if len(sys.argv) != 2:
		print("Usage: wordcount <file>", file=sys.stderr)
		exit(-1)
 
	sc = SparkContext()
	lines = sc.textFile(sys.argv[1],1)
	lines = lines.mapPartitions(lambda x:reader(x))
	weekend = [5,6,12,13,19,20,26,27]

	wendc = lines.map(lambda x: (x[2],1  if int(x[1].split("-")[2]) in weekend else 0   )).reduceByKey(lambda x,y: float(x)+float(y))
	wdayc = lines.map(lambda x: (x[2],1  if int(x[1].split("-")[2]) not in weekend else 0   )).reduceByKey(lambda x,y: float(x)+float(y))

	wend_avg = wendc.map(lambda x: (x[0],float(x[1])/8.0))
	wday_avg = wdayc.map(lambda x: (x[0],float(x[1])/23.0))

	r = wend_avg.join(wday_avg)
	r.map(lambda x: "{0}\t{1:.2f}, {2:.2f}".format(x[0],float(x[1][0]),float(x[1][1]))).saveAsTextFile("task7.out")
    #m = triplet.filter(lambda x: x[1] in ["2016-03-05","2016-03-06","2016-03-12","2016-03-13","2016-03-19","2016-03-20","2016-03-26","2016-03-27"])
    #m = m.map(lambda xy:(xy[0],xy[2])).reduceByKey(add).map(lambda xy:(xy[0],round((xy[1]/8),2)))
    
    #n =  triplet.filter(lambda x: x[1] not in ["2016-03-05","2016-03-06","2016-03-12","2016-03-13","2016-03-19","2016-03-20","2016-03-26","2016-03-27"])
    #n = n.map(lambda xy:(xy[0],xy[2])).reduceByKey(add).map(lambda xy:(xy[0],round((xy[1]/23),2)))
    
    #r = m.fullOuterJoin(n)
    ##r = r.mapValues(lambda y: (0,0) if (y[0]=="None" and y[1]=="None") else (y[0],y[1]) )
    #r = r.mapValues(lambda y: (0,y[1]) if (y[0]=="None") else  (y[0],y[1]))
    #r = r.mapValues(lambda y: (y[0],0) if (y[1]=="None") else (y[0],y[1]))                  
    #r = r.map(lambda xy:str(xy[0])+"\t"+str(xy[1][0])+", "+str(xy[1][1]))
    #r.saveAsTextFile("task7.out") 
	sc.stop()
