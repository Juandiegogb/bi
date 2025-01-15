from pyspark.sql import SparkSession , DataFrame

spark : SparkSession = SparkSession.builder.appName("test").getOrCreate()

df: DataFrame = spark.createDataFrame([1000],("time"))
print(df)
df.show()
