from pyspark.sql import SparkSession
import sys
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Python Spark SQL basic example4444").config("spark.some.config.option", "some-value").getOrCreate()

parking= spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
parking.createOrReplaceTempView("parking")

parks = spark.sql("select registration_state, count(registration_state) as cnt from parking group by registration_state")
parks.createOrReplaceTempView("parks")

parks = spark.sql("select 'Other' as state, sum(cnt) as cnt from parks where registration_state != 'NY' UNION select 'NY' as state, cnt from parks where registration_state == 'NY'")
parks.createOrReplaceTempView("parks")
parks = spark.sql("select state, cnt from parks order by state")

parks.select(format_string('%s\t%d',parks.state, parks.cnt)).write.save("task4-sql.out",format="text")
