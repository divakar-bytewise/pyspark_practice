from pyspark.sql import SparkSession
import os

os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'

spark=SparkSession.builder.appName("Transformation and Action").master("local[*]").getOrCreate()

data=[(1,'Divakar',97,'CSE'),(2,'Edwin',99,'ISE'),(3,'Stephan',90,'ECE'),(3,'Stewart',98,'AIML')]
columns=['USN','Name','Marks','Department']

df=spark.createDataFrame(data,columns)

Marks_filter=df.filter(df.Marks>90)

selected_data=Marks_filter.select('Name','Marks','Department')

selected_data.show()
spark.stop()