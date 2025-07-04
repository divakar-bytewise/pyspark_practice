from pyspark.sql import Row,SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'

spark=SparkSession.builder.appName("Column Class Operations").getOrCreate()
col1=lit('Appu')
print(type(col1))
data=[('Divakar',23,23000),('Edwin',22,24000),('Stewart',22,25000)]
schema=['Name','Age','Salary']
df=spark.createDataFrame(data,schema)
df1=df.withColumn('Gender',lit('Male'))
df2=df.select(df.Name)
df3=df.select(df['Age'])
df4=df1.select(col('Gender'))
df.show()
df1.show()
df2.show()
df3.show()
df4.show()

data1=[('Divakar',23,23000,('DE','HSR')),('Edwin',22,24000,('DE','HOSUR'))
    ,('Stewart',22,25000,('DE','HOSUR'))]
pstruct=StructType([StructField('Role',StringType()),
                    StructField('Location',StringType())])
schema=StructType([StructField('Name',StringType()),
                   StructField('Age',IntegerType()),
                   StructField('Salary',IntegerType()),
                   StructField('Personal_details',pstruct)])
df5=spark.createDataFrame(data1,schema)
df5.show()
df5.printSchema()
df6=df5.select(df5.Personal_details.Role).show()