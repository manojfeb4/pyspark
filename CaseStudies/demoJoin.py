from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, broadcast

spark = SparkSession.builder.appName("test").getOrCreate()
pplData = [Row(101, "manoj", 20), Row(102, "magesh", 30), Row(103, "vaishnav", 12), Row(104, "saindhavi", 8)]
pplRdd = spark.sparkContext.parallelize(pplData)
ppl = pplRdd.map(lambda x: Row(roll=int(x[0]), name=x[1], age=int(x[2])))
pplDf = spark.createDataFrame(ppl)
pplDf.show()

subjData = [Row(101, "maths"), Row(101, "english"), Row(102, "science"), Row(103, "social")]
subjRdd = spark.sparkContext.parallelize(subjData)
subj = subjRdd.map(lambda x: Row(roll= int(x[0]), subject=x[1]))
subjDf = spark.createDataFrame(subj)
subjDf.show()

innerJoinDf=pplDf.alias("t1").join(broadcast(subjDf).alias("t2"), col("t1.roll") == col("t2.roll"), "inner")
innerJoinDf.explain()
innerJoinDf.show()

leftSemiJoinDf=pplDf.alias("t1").join(subjDf.alias("t2"), col("t1.roll") == col("t2.roll"), "semi")
leftSemiJoinDf.show()

leftJoinDf=pplDf.alias("t1").join(subjDf.alias("t2"), col("t1.roll") == col("t2.roll"), "leftouter")
leftJoinDf.show()

leftAntiDf=pplDf.alias("t1").join(subjDf.alias("t2"), col("t1.roll") == col("t2.roll"), "anti")
leftAntiDf.show()



