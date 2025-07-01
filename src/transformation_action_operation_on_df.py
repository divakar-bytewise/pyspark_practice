from pyspark.sql import SparkSession
import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'

spark= SparkSession.builder.appName("Example Programs").master("local[*]").getOrCreate()

data=[(1,'Divakar',97,'CSE'),
      (2,'Edwin',99,'ISE'),
      (3,'Stephan',90,'ECE'),
      (4,'Stewart',98,'AIML'),
      (5,'Dominic',94,'CSE')]

column_name=['USN','Name','Marks','Department']

df= spark.createDataFrame(data,column_name)

filtered_data=df.filter(df.Marks>90)
selected_data=filtered_data.select('USN','Name','Marks')

with_precentage=selected_data.withColumn("Precentage",selected_data.Marks/100*100)
grouped_data=df.groupby('Department').avg('Marks')
grouped_data.show()
selected_data.show()
with_precentage.show()
print("Total Students: ",df.count())
spark.stop()