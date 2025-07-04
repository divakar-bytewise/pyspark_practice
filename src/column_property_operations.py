from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Column Operations").getOrCreate()

data = [(1, 'Divakar', 95), (2, 'Appu', 88)]
columns = ['ID', 'Name', 'Marks']

df = spark.createDataFrame(data, columns)

print(df.columns)
for col in df.columns:
    print(f"The column name= {col}")