from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'

spark=SparkSession.builder.appName("Absolute Operations").getOrCreate()

data=[('Divakar',23,-23090.123,'DE'),('Edwin',22,-24500.432,'DS'),('Stewart',22,25000.232,'DE')]
schema=['Name','Age','Salary','Role']
df=spark.createDataFrame(data,schema)
df1=df.select('Name','Salary',abs('Salary').alias('Absolute_salary'))
df1.show()