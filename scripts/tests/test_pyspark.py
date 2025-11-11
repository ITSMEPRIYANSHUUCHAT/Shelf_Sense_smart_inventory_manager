from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LocalPySparkTest").getOrCreate()
df = spark.createDataFrame([(1, "test")], ["id", "value"])
df.show()
spark.stop()