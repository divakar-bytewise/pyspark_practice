from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'

spark=SparkSession.builder.appName("arraytype column operations").getOrCreate()

data=[(1,'Dominic',[92,90]),(2,'Pradeep',[67,54])]
data1=[(101,104),(107,204)]
schema=StructType([StructField('Id',IntegerType()),
                   StructField('Name',StringType()),
                   StructField('Marks',ArrayType(IntegerType()))])
#column=['Id','Name','Marks']
df=spark.createDataFrame(data,schema)
df1=spark.createDataFrame(data1,['num1','num2'])
result=df.withColumn('ADE',df.Marks[0])
result1=df1.withColumn('Numbers',array(df1.num1,df1.num2))

df.show()
df.printSchema()
result.show()
df1.show()
result1.show()