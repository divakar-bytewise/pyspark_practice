from pyspark.sql import SparkSession

import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'

spark=SparkSession.builder.appName("CSV file").master("local[*]").getOrCreate()

#reading single file
df=spark.read.csv("data/Department-Q1.csv",header=True)

#reading multiple files.
df1=spark.read.csv(["data/Department-Q1.csv","data/Country-Q1.csv"], header=True)


df.show()
df1.show()
spark.stop()