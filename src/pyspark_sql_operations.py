from pyspark.sql import *
from pyspark.sql.types import *
import os

os.environ['PYSPARK_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\DivakarC\AppData\Local\Programs\Python\Python311\python.exe'


spark=SparkSession.builder.appName("Pyspark SQL").getOrCreate()

data=[(1,'Divakar',97,'CSE'),(2,'Appu',85,'AIML'),(3,'VJ',98,'ISE')]
struct=StructType([StructField('id',IntegerType(),False),
                   StructField('name',StringType(),False),
                   StructField('score',IntegerType(),False),
                   StructField('department',StringType(),False)])
df= spark.createDataFrame(data,schema=struct)
df.createOrReplaceTempView("Students")
result=spark.sql("Select * from Students where score>90")
r1=spark.sql("select avg(score) as Avg_marks from Students")

departments = [
    ("CSE", "Computer Science"),
    ("ISE", "Information Science")]
dept_df = spark.createDataFrame(departments, ["department", "dept_name"])
dept_df.createOrReplaceTempView("departments")
r2=spark.sql("""SELECT s.name, s.score, d.dept_name
                FROM students s
                JOIN departments d ON s.department = d.department""")
r2.show()
r1.show()
result.show()