from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit
import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'

spark= SparkSession.builder.appName("Example Programs").master("local[*]").getOrCreate()

data=[(1,'Divakar','97','CSE'),
      (2,'Edwin','99','ISE'),
      (3,'Stephan','90','ECE'),
      (4,'Stewart','98','AIML'),
      (5,'Dominic','94','CSE')]

column_name=['USN','Name','Marks','Department']

df= spark.createDataFrame(data,column_name)

result=df.withColumn('Marks',df.Marks.cast('Integer'))
result1=result.withColumn('Marks',col('Marks')*0.2)
result2=result1.withColumn('Down_graded_marks',col('Marks'))
result3=result.withColumn('College',lit('The Oxford'))
# result1=df.withColumn('Marks',col=col('Marks').cast('Integer'))
result.show()
result.printSchema()
result1.show()
result1.printSchema()
result2.show()
result2.printSchema()
result3.show()
result3.printSchema()