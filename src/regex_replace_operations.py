from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'

spark = SparkSession.builder.appName("Regular Expression Replace Operations").getOrCreate()

data = [(1, "Divakar-Appu-Python"), (2, "ICC-World-Test")]

columns = ["ID", "FullName"]

df = spark.createDataFrame(data,columns)

df1= df.select("FullName", regexp_replace("FullName","[^a-z,A-Z]","").alias("SplitName"))
df2= df.select("FullName", regexp_replace("FullName","[^a-z,A-Z]"," ").alias("SplitName"))
df1.show(truncate=False)
df2.show(truncate=False)