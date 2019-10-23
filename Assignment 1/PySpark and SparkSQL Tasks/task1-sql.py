import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("Python Spark SQL Task1").config("spark.some.config.option", "some-value").getOrCreate()

parking = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
openv = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[2]) 

parking.createOrReplaceTempView("parking")
openv.createOrReplaceTempView("openv")
    
result = spark.sql("select p.summons_number,p.plate_id, p.violation_precinct, p.violation_code, p.issue_date from parking p left join openv o on p.summons_number = o.summons_number where  o.summons_number is null")
result.select(format_string('%d\t%s,%d,%d,%s',result.summons_number,result.plate_id,result.violation_precinct,result.violation_code,date_format(result.issue_date,'yyyy-MM-dd'))).write.save("task1-sql.out",format="text")
    
