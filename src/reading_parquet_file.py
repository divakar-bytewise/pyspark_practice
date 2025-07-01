from pyspark.sql import SparkSession
import os

os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'


spark=SparkSession.builder.appName("Reading parquet file").getOrCreate()
df= spark.read.parquet("data/parquet_1.parquet")
df1= spark.read.parquet("data/parquet_2.parquet")
df.show()
df1.show()
print("The total datasets=",df1.count())