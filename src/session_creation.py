from pyspark.sql import SparkSession

spark_session=(
    SparkSession
    .builder
    .appName("SparkSession Introduction")
    .master("local[*]")
    .getOrCreate()
)
df=spark_session.read.format("csv").option("inferSchema",True).load("Employee-Q1.csv",header=True,)
df.printSchema()
df.show()
spark_session.stop()


