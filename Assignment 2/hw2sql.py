#!/usr/bin/env python
# coding: utf-8

import sys
import pyspark
import string

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import *

if __name__ == "__main__":

    sc = SparkContext()

    spark = SparkSession \
        .builder \
        .appName("hw2sql") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    sqlContext = SQLContext(spark)

    # get command-line arguments
    inFile = sys.argv[1]
    supp = sys.argv[2]
    conf = sys.argv[3]
    prot = sys.argv[4]

    print ("Executing HW2SQL with input from " + inFile + ", support=" +supp + ", confidence=" + conf + ", protection=" + prot)

    pp_schema = StructType([
            StructField("uid", IntegerType(), True),
            StructField("attr", StringType(), True),
            StructField("val", IntegerType(), True)])

    Pro_Publica = sqlContext.read.format('csv').options(header=False).schema(pp_schema).load(inFile)
    Pro_Publica.createOrReplaceTempView("Pro_Publica")
    sqlContext.cacheTable("Pro_Publica")

    # compute frequent itemsets of size 1, store in F1(attr, val)
    query = "select attr, val, count(*) as supp \
              from Pro_Publica \
              group by attr, val \
             having count(*) >= "  + str(supp)
    F1 = spark.sql(query)
    F1.createOrReplaceTempView("F1")
    #F1.show()
    
    query = "select * from Pro_Publica where attr != 'vdecile' "
    Pro_Publica_Attr = spark.sql(query)
    Pro_Publica_Attr.createOrReplaceTempView("Pro_Publica_Attr")

    query = "select * from Pro_Publica where attr = 'vdecile'"
    Pro_Publica_Vdecile = spark.sql(query)
    Pro_Publica_Vdecile.createOrReplaceTempView("Pro_Publica_Vdecile")

    # uid, attr, val, vd, vdval  
    query = "select p.uid, p.attr as attr, p.val as val, q.attr as vd, q.val as vdval \
             from Pro_Publica_Attr p join Pro_Publica_Vdecile q on p.uid = q.uid"
    temp = spark.sql(query)
    temp.createOrReplaceTempView("temp")
     
    # attr, val, vd, vdval, supp
    query = "select attr,val,vd,vdval,count(*) as supp \
             from temp\
             group by attr,val,vd,vdval \
             having count(*) >=" +str(supp)+"\
             order by attr,val"
    temp2 = spark.sql(query)
    temp2.createOrReplaceTempView("temp2")

    # attr, val, total 
    query = "select attr,val,count(*) as total \
             from temp\
             group by attr,val"
    temp3 = spark.sql(query)
    temp3.createOrReplaceTempView("temp3")

    # attr1, val1, attr2, val2, supp, conf
    query = "select temp2.attr as attr1 ,temp2.val as val1,vd as attr2,vdval as val2,supp,supp/total as conf \
             from temp2 join temp3 on temp2.attr = temp3.attr and temp2.val = temp3.val \
             where supp/total >= "+str(conf)+"\
             order by attr1,val1"
    R2 = spark.sql(query)
    R2.createOrReplaceTempView("R2")

    #Computing R3
    query = "select p1.uid, p1.attr as attr1, p1.val as val1, p2.attr as attr2, p2.val as val2, p3.attr as attr3, p3.val as val3 \
		from Pro_Publica_Attr p1 join Pro_Publica_Attr p2 on p1.uid =p2.uid join Pro_Publica_Vdecile p3 on p1.uid = p3.uid \
		where p1.attr < p2.attr and \
                (p1.attr,p1.val) in (select attr,val  from F1)  and \
                (p2.attr,p2.val) in (select attr,val from F1) and \
                (p3.attr,p3.val) in (select attr,val from F1)"
    tempo = spark.sql(query)
    tempo.createOrReplaceTempView("tempo")

    # (attr1,val1,attr2,val2,attr3,val3), supp
    query =    "select attr1, val1, attr2, val2, attr3, val3, count(*) as supp \
		from tempo \
                group by attr1,val1,attr2,val2,attr3,val3 \
                having count(*) >= "+str(supp)

    temp4 = spark.sql(query)
    temp4.createOrReplaceTempView("temp4")
    
    # (attr1,val1,attr2,val2), total
    query = "select p1.attr1, p1.val1, p1.attr2, p1.val2, count(*) as total  \
             from tempo p1 \
             group by p1.attr1,p1.val1,p1.attr2,p1.val2 "
    temp6 = spark.sql(query)
    temp6.createOrReplaceTempView("temp6")
    
    query = "select t1.attr1, t1.val1, t1.attr2, t1.val2, t1.attr3, t1.val3, supp, supp/total as conf from \
             temp4 t1 join temp6 t2 on t1.attr1 = t2.attr1 and t1.val1 = t2.val1 and t1.attr2 = t2.attr2 and t1.val2= t2.val2 \
             where supp/total >= "+str(conf)+"\
             order by t1.attr1,t1.val1,t1.attr2,t1.val2"
    R3 = spark.sql(query)
    R3.createOrReplaceTempView("R3")

    # Compute PD_R3(attr1, val1, attr2, val2, attr3, val3, supp, conf, prot) 
    # attr1, val1, attr2, val2, attr3, val3, supp
    query = "select  attr1, val1,  attr2,  val2,  attr3,  val3, supp, conf from \
             R3 \
             where attr2 = 'race'" 

    temp7 = spark.sql(query)
    temp7.createOrReplaceTempView("temp7")

    query = "select t1.attr1, t1.val1, t1.attr2, t1.val2, t1.attr3, t1.val3, t1.supp, t1.conf, t1.conf/R2.conf as prot from \
             temp7 t1 join R2 on t1.attr1 = R2.attr1 and t1.val1 = R2.val1 and t1.attr3 = R2.attr2 and t1.val3= R2.val2 \
             where t1.conf/R2.conf >= "+str(prot)+"\
             order by t1.attr1,t1.val1,t1.attr2,t1.val2"
    PD_R3 = spark.sql(query)
    PD_R3.createOrReplaceTempView("PD_R3")


    R2.select(format_string('%s,%s,%s,%s,%d,%.2f',R2.attr1,R2.val1,R2.attr2,R2.val2,R2.supp,R2.conf)).write.save("r2.out",format="text")
    R3.select(format_string('%s,%s,%s,%s,%s,%s,%d,%.2f',R3.attr1,R3.val1,R3.attr2,R3.val2,R3.attr3,R3.val3,R3.supp,R3.conf)).write.save("r3.out",format="text")
    PD_R3.select(format_string('%s,%s,%s,%s,%s,%s,%d,%.2f,%.2f',PD_R3.attr1,PD_R3.val1,PD_R3.attr2,PD_R3.val2,PD_R3.attr3,PD_R3.val3,PD_R3.supp,PD_R3.conf,PD_R3.prot)).write.save("pd-r3.out",format="text")
   
    sc.stop()

