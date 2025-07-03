from pyspark.sql import SparkSession
import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'

spark=SparkSession.builder.appName("Collect operation").getOrCreate()

data=[(1,'Dominic',92),(2,'Praveen',67),(3,'Pradeep',69)]
df=spark.createDataFrame(data,['Id','Name','Marks'])
rows=df.take(2)
for row in rows:
    print(f"Id:{row['Id']}, Name:{row['Name']}")