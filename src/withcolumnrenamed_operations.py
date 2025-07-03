from pyspark.sql import SparkSession
import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'

spark=SparkSession.builder.appName("Withcolumnrenamed operations").getOrCreate()

data=[(1,'Dominic',92),(2,'Pradeep',67)]
df=spark.createDataFrame(data,['Id','Name','Marks'])

df1=df.withColumnRenamed('Marks','Students_marks')
df.show()
df1.show()
df1.printSchema()