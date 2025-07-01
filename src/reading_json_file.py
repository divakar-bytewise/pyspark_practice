from pyspark.sql import SparkSession
import os

os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'

spark=SparkSession.builder.appName("Reading json file").master("local[*]").getOrCreate()

#reading multiline json file.
df = spark.read.json("data/demo_1.json",multiLine=True)
#reading normal single line json file.
df1 = spark.read.json("data/demo_2.json")
#reading the 2 json file having the same schema.
df2= spark.read.json(["data/demo_2.json","data/demo_3.json"])
#this is the one more way to read the file.
df3 =spark.read.format("json").load(["data/demo_2.json","data/demo_3.json"])
df.show()
df1.show()
df2.show()
df3.show()
