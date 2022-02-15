from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, sum

spark = SparkSession.builder.appName("empAnalysis").master("local[4]").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "100")
dirPath = "/users/manmanok/documents/test_data/inputFiles/"
empDf = spark.read.csv(path = dirPath + "/employee.csv", header = True, sep=",", inferSchema=True, nullValue="-")
deptDf = spark.read.csv(path = dirPath + "/dept.csv", header = True, sep=",", inferSchema=True, nullValue="-")

innerJoinDf = empDf.alias("t1").join(deptDf.alias("t2"), col("t1.department_id") == col("t2.department_id"), "inner")
innerJoinDf = innerJoinDf.select("employee_id", "location_id").coalesce(50)

empDf.printSchema()
salaryDf = empDf.groupby("department_id").sum("salary").orderBy("department_id")
salaryDf.show()

salaryWindow = Window.partitionBy("department_id").orderBy("salary").rowsBetween(Window.unboundedPreceding,Window.currentRow)
windowSalary = sum("salary").over(salaryWindow)
salaryDf = empDf.select("department_id","salary", windowSalary.alias("windowSalary"))
salaryDf.show()

