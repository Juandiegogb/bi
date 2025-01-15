"""
Esta es una ETL con Python y Pyspark
"""

from time import time
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.sql import SparkSession
from config import  pg

started_at = time()
spark: SparkSession = (
    SparkSession.builder.appName("ETL")
    .config("spark.executor.cores", "4")
    .config("spark.executor.memory", "4g")
    .getOrCreate()
)

pg_url = f"jdbc:postgresql://{pg['host']}:{pg['port']}/{pg['db']}"
pg_properties: dict = {
    "user": pg["user"],
    "password": pg["pwd"],
    "driver": "org.postgresql.Driver",
}
ended_at = time()
total_time = round((ended_at - started_at) / 60, 2)

schema = StructType([StructField("time", DoubleType(), True)])
time_df = spark.createDataFrame([(total_time,)], schema)

time_df.write.mode("append").jdbc(pg_url, "time_stats", properties=pg_properties)
print("Stats table loaded to stage")