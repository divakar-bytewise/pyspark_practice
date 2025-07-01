from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os

os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'

spark=(SparkSession.builder.appName("DataFrame Creation").master("local[*]").getOrCreate())
data=[(1,'Diva'),(2,'Appu')]
# schema=StructType([StructField(name='id',dataType=IntegerType()),
#                    StructField(name='name',dataType=StringType())])

#df= spark.createDataFrame(data,schema)
df=spark.createDataFrame(data=data,schema=['id','name'])
df.show()
df.printSchema()
spark.stop()
