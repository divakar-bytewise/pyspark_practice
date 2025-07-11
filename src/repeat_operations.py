from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'

spark = SparkSession.builder.appName("Repeat Operations").getOrCreate()

data = [(1, "Divakar"), (2, "Edwin")]

columns = ["ID", "FullName"]

df = spark.createDataFrame(data,columns)

df1=df.select('FullName',repeat('FullName',3).alias('Repeated'))
df1.show(truncate=False)