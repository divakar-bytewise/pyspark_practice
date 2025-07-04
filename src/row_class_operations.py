from pyspark.sql import Row,SparkSession
import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'

spark=SparkSession.builder.appName("Row Class Operations").getOrCreate()
data=Row('Diva',24)
data1=Row(Name='Appu',Age='23')
print(data[0]+ " "+str(data[1]))
print(data1.Name+" "+str(data1.Age))
d=[data1]
df=spark.createDataFrame(d)
df.show()

data2=Row('Name','Age','Salary')
r1=data2('Edwin','22','2300')
r2=data2('Stewart','22','2400')
print(r1.Name+" "+r2.Name)
d1=[r1,r2]
df1=spark.createDataFrame(d1)
df1.show()

data3=[Row(Name='Diva',Prop=Row(Age='24',Gender='M'))]
df2=spark.createDataFrame(data3)
df2.show()