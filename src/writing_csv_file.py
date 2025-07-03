from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'

spark=SparkSession.builder.appName("Writing csv file").master("local[*]").getOrCreate()

data=[(1,'Zepto'),(2, 'Instamart')]
schema=StructType([StructField('id',IntegerType(),False),
                   StructField('name',StringType(),False)])
df=spark.createDataFrame(data,schema)
df.show()
df.coalesce(1).write.option("header",True).mode('overwrite').format("csv").save("data/df_write")
# df1=spark.read.format("csv").load("data/df_write",header=True)
#
# df1.show()