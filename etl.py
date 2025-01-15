from pyspark.sql import SparkSession, DataFrame
from config import mssql, pg
from time import time

started_at = time()


spark: SparkSession = (
    SparkSession.builder.appName("ETL")
    .config("spark.executor.cores", "4")
    .config("spark.executor.memory", "4g")
    # .master("spark://192.168.1.240:7077")
    .getOrCreate()
)

mssql_url = (
    f"jdbc:sqlserver://{mssql['host']}:{mssql['port']};databaseName={mssql['db']};"
    "encrypt=true;trustServerCertificate=true"
)

mssql_properties: dict = {
    "user": mssql["user"],
    "password": mssql["pwd"],
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}

pg_url = f"jdbc:postgresql://{pg['host']}:{pg['port']}/{pg['db']}"

print(pg_url)
pg_properties: dict = {
    "user": pg["user"],
    "password": pg["pwd"],
    "driver": "org.postgresql.Driver",
}


print("Extracting data from DB")
df: DataFrame = spark.read.jdbc(
    mssql_url, "behavior_october", properties=mssql_properties
)

total_rows = df.count()


brands = df.select("brand").distinct().count()
users = df.select("user_id").distinct().count()
products = df.select("product_id").distinct().count()
views = df.filter(df["event_type"] == "view").count()
rows = df.count()



stats_columns = ["stat", "value"]
stats_data = [
    ("brands quantity", brands),
    ("users", users),
    ("products", products),
    ("total views", views),
    ("total rows", rows),
]

stats_df = spark.createDataFrame(stats_data, stats_columns)
stats_df.write.mode("overwrite").jdbc(pg_url, table="stats", properties=pg_properties)


print("Stats table loaded to stage")

ended_at = time()
total_time = ended_at - started_at

print(f"This job took {round(total_time / 60)} minutes and processed {total_rows} rows")
