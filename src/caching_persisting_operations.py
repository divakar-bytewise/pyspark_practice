from pyspark.sql import SparkSession
from pyspark import StorageLevel
import time
import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'

spark=SparkSession.builder.appName("caching and persisting operations").getOrCreate()

df=spark.read.option('header','true').csv("data/Employee-Q1.csv")

df.persist(StorageLevel.MEMORY_AND_DISK)
start=time.time()
df.groupby("Department").count().show()
df.select("EmployeeID","EmployeeName").distinct().show()
print("With caching and persisting",time.time()-start,"seconds")
df.unpersist
