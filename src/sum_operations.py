from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'

spark=SparkSession.builder.appName("Sum Operations").getOrCreate()

data=[('Divakar',23,23000,'DE'),('Edwin',22,24000,'DS'),('Stewart',22,25000,'DE')]
schema=['Name','Age','Salary','Role']
df=spark.createDataFrame(data,schema)
df1=df.select(sum("Salary").alias("Total_Expense"))
df2=df.groupby('Role').agg(sum('Salary').alias('Total_salary'))
df1.show()
df2.show()