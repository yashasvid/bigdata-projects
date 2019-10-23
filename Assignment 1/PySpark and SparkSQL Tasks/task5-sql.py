import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("Python Spark SQL Task5").config("spark.some.config.option", "some-value").getOrCreate()

parking = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
parking.createOrReplaceTempView("parking")
    
result = spark.sql("select plate_id, registration_state, count(*) as ctr from parking group by plate_id,registration_state order by ctr DESC limit 1")
result.select(format_string('%s, %s\t%d',result.plate_id,result.registration_state,result.ctr)).write.save("task5-sql.out",format="text")
    
