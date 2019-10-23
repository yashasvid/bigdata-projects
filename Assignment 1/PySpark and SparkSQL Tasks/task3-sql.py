import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Python Spark SQL Task3").config("spark.some.config.option", "some-value").getOrCreate()

o = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1]) 
o.createOrReplaceTempView("o")

result = spark.sql("select license_type, sum(amount_due) as amt_sum, avg(amount_due) as amt_avg from o group by license_type")
result.select(format_string('%s\t%.2f, %.2f',result.license_type,result.amt_sum,result.amt_avg)).write.save("task3-sql.out",format="text")
   
