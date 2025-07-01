from pyspark.sql import SparkSession

import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'

spark=SparkSession.builder.appName("Spark Context").getOrCreate()

sc=spark.sparkContext

rdd=sc.parallelize([1,2,3,4,5,6,7])
rdd1=rdd.map(lambda x:x**2).collect()
rdd2=rdd.reduce(lambda x,y:x+y)

print("Square:",rdd1)
print("Sum:",rdd2)