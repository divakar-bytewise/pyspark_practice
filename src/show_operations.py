from pyspark.sql import SparkSession
import os

os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'

spark=SparkSession.builder.appName("Show operations").getOrCreate()

data=[(1,'Divakar','asdfghjkl;iuytrewrghjkhgfdfghjk'),(2,'Appu','gvbjklbvxcvbgtgbjhgfcjncndej'),
      (3,'Edwin','gvbjhnkhgjkhgjkhgvbcfvbnhjjhbvjkbh')]
columns=['id','name','raw_data']

df=spark.createDataFrame(data,columns)
#df.show()
# df.show(truncate=False)
# df.show(n=1)
df.show(vertical=True)