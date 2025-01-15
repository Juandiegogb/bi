from time import time

from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.sql import SparkSession, DataFrame, functions
from pyspark.storagelevel import StorageLevel
from config import mssql, pg

# Medir el tiempo total del job
started_at = time()

# Crear sesión de Spark con configuraciones optimizadas
spark: SparkSession = (
    SparkSession.builder.appName("Optimized ETL")
    .config("spark.executor.cores", "6")  # Ajustar según los recursos del clúster
    .config("spark.executor.memory", "8g")  # Aumentar memoria si es necesario
    .config("spark.sql.shuffle.partitions", "100")  # Ajustar el número de particiones
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

# URLs y propiedades de las bases de datos
mssql_url = (
    f"jdbc:sqlserver://{mssql['host']}:{mssql['port']};databaseName={mssql['db']};"
    "encrypt=true;trustServerCertificate=true"
)
mssql_properties = {
    "user": mssql["user"],
    "password": mssql["pwd"],
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}

pg_url = f"jdbc:postgresql://{pg['host']}:{pg['port']}/{pg['db']}"
pg_properties = {
    "user": pg["user"],
    "password": pg["pwd"],
    "driver": "org.postgresql.Driver",
}

# Extracción de datos con filtrado temprano y selección de columnas específicas
print("Extracting data from DB")
extraction_time = time()

query = """
SELECT brand, user_id, product_id, event_type
FROM behavior_october
WHERE event_type IN ('view', 'click')  -- Filtrar eventos relevantes
"""
df: DataFrame = spark.read.jdbc(
    mssql_url,
    f"({query}) as subquery",
    properties=mssql_properties
)

extraction_time = (time() - extraction_time) / 60
print(f"The extraction of the data took {extraction_time:.2f} minutes")

# Persistir datos para reutilización
df.persist(StorageLevel.MEMORY_AND_DISK)

# Transformaciones optimizadas
print("Transforming data")
transformation_time = time()

# Usar agregaciones optimizadas
df_stats = df.groupBy().agg(
    functions.approx_count_distinct("brand").alias("brands_quantity"),  # Aproximado para optimizar
    functions.approx_count_distinct("user_id").alias("users"),
    functions.approx_count_distinct("product_id").alias("products"),
    functions.sum(
        functions.when(df["event_type"] == "view", 1).otherwise(0)
    ).alias("total_views"),
    functions.count("*").alias("total_rows"),
).collect()[0]

transformation_time = (time() - transformation_time) / 60
print(f"The transformation of the data took {transformation_time:.2f} minutes")

# Crear un DataFrame para estadísticas
stats_data = [
    ("brands quantity", df_stats["brands_quantity"]),
    ("users", df_stats["users"]),
    ("products", df_stats["products"]),
    ("total views", df_stats["total_views"]),
    ("total rows", df_stats["total_rows"]),
]
stats_df = spark.createDataFrame(stats_data, ["stat", "value"])

# Escribir las estadísticas en PostgreSQL
print("Loading stats table to PostgreSQL")
stats_df.repartition(1).write.mode("overwrite").jdbc(
    pg_url, table="stats", properties=pg_properties
)

# Liberar memoria persistida
df.unpersist()

# Medir el tiempo total
ended_at = time()
total_time = round((ended_at - started_at) / 60,2)

schema = StructType([StructField("time", DoubleType(), True)])
time_df = spark.createDataFrame([(total_time,)], schema)

time_df.write.mode("append").jdbc(pg_url,"time_stats" , properties=pg_properties)

print(f"This job took {total_time:.2f} minutes and processed {df_stats['total_rows']} rows")


