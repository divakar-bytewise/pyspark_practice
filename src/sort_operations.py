from pyspark.sql import SparkSession
import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'

spark = SparkSession.builder.appName("Sort Operations").getOrCreate()

data = [("Diva", 95), ("Appu", 88), ("Zara", 76), ("John", 62)]
columns = ["name", "marks"]

df = spark.createDataFrame(data, columns)

result=df.sort("marks")
result1=df.sort(df.marks.desc(),df.name.asc())
result.show()
result1.show()