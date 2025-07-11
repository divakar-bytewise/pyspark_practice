from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'

spark=SparkSession.builder.appName("Upper and Lower Operations").getOrCreate()

data=[('Divakar',23,23090,'DE'),('Edwin',22,24500,'DS'),('Stewart',22,25000,'DE')]
schema=['Name','Age','Salary','Role']
df=spark.createDataFrame(data,schema)
df1=df.select('Name',upper('Name').alias('Uppercase_Name'))
df2=df.select('Name',lower('Name').alias('Lower_Name'))
df1.show()
df2.show()