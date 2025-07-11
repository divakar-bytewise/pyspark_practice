from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'


spark = SparkSession.builder.appName("SubstringIndex Operations").getOrCreate()

data = [("divakar.appu.python",), ("hello.world.test",)]
columns = ["full_string"]
df = spark.createDataFrame(data, columns)
df1=df.select("full_string", substring_index("full_string", ".", 1).alias("First Part"))
df2=df.select("full_string", substring_index("full_string", ".", -1).alias("Last Part"))

df1.show()
df2.show()