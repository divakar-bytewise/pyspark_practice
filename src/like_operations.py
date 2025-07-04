from pyspark.sql import SparkSession
import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'

spark=SparkSession.builder.appName("Like operations").getOrCreate()
data=[('Divakar',),('Edwin',),('David',),('Dinesh',),('Stewart',),
      ('Arya',),('Appu',),('Arun',),('Dominic',)]
column=['Name']
df=spark.createDataFrame(data,column)

result=df.filter(df.Name.like("D%"))
result1=df.filter(df.Name.like("%a"))
result2=df.filter(df.Name.like("_p%"))
result3=df.filter(~df.Name.like("D%"))
result.show()
result1.show()
result2.show()
result3.show()

