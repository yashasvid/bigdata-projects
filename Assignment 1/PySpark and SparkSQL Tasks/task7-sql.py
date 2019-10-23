import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Python Spark SQL Task7").config("spark.some.config.option", "some-value").getOrCreate()

parking = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])

parking.createOrReplaceTempView("parking")

res =spark.sql("select v1.violation_code,(v1.weekend_count/8) as weekend_Avg,(v2.weekday_Count/23) as weekday_Avg from (select violation_code, sum(index) as weekend_count from (select violation_code, case when DAY(issue_date) IN (5,6,12,13,19,20,26,27) then 1 else 0 end as index from parking) group by violation_code) v1, (select violation_code, sum(index) as weekday_Count from (select violation_code, case when DAY(issue_date) NOT IN (5,6,12,13,19,20,26,27) then 1 else 0 end as index from parking) group by violation_code) v2 where v1.violation_code = v2.violation_code")
res.createOrReplaceTempView("res")

res.select(format_string('%s\t%.2f, %.2f',res.violation_code,res.weekend_Avg,res.weekday_Avg)).write.save("task7-sql.out",format="text")






























#res = spark.sql("SELECT violation_code, weekendaverage from t1 union select violation_code, weekendaverage from t2") 

#res.select(format_string('%d\t%.2f',res.violation_code,res.weekendaverage)).write.save("task7-sql.out",format="text")
    
