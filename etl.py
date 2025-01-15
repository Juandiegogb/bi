"""
Esta es una ETL con Python y Pyspark
"""

from time import time
from pyspark.sql import SparkSession
from config import mssql, pg
from pyspark.sql.types import StructType, StructField, DoubleType


def test_jdbc_connection(url: str, properties: dict, query: str = "SELECT 1 AS test_col"):
    """
    Prueba una conexión JDBC ejecutando una consulta simple.

    Args:
        url (str): La URL JDBC de la base de datos.
        properties (dict): Las propiedades JDBC (usuario, contraseña, driver).
        query (str): La consulta para probar la conexión. Por defecto, 'SELECT 1 AS test_col'.

    Returns:
        bool: True si la conexión es exitosa, False en caso contrario.
    """
    try:
        # Ejecutar la consulta usando Spark JDBC
        spark.read.jdbc(url, f"({query}) AS test", properties=properties).count()
        print(f"JDBC connection to {url.split(':')[1]} passed")
        return True
    except Exception as e:
        print(f"Error connecting via JDBC to {url.split(':')[1]}: {e}")
        return False


# Medir el tiempo total del job
started_at = time()

# Crear la sesión de Spark
spark: SparkSession = (
    SparkSession.builder.appName("ETL")
    .config("spark.executor.cores", "4")
    .config("spark.executor.memory", "4g")
    .getOrCreate()
)

# Configuración de las bases de datos
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
pg_properties: dict = {
    "user": pg["user"],
    "password": pg["pwd"],
    "driver": "org.postgresql.Driver",
}

# Verificar conexiones JDBC
if not test_jdbc_connection(mssql_url, mssql_properties):
    print("Failed to connect to MSSQL via JDBC. Aborting job.")
    exit(1)

if not test_jdbc_connection(pg_url, pg_properties):
    print("Failed to connect to PostgreSQL via JDBC. Aborting job.")
    exit(1)

# Extracción de datos
print("Extracting data from DB")
extraction_time = time()
df = spark.read.jdbc(
    mssql_url, "behavior_october", properties=mssql_properties
)

extraction_time = (time() - extraction_time) / 60
print(f"The extraction of the data took {extraction_time:.2f} minutes")

# Transformación de datos
transformation_time = time()

total_rows = df.count()
brands = df.select("brand").distinct().count()
users = df.select("user_id").distinct().count()
products = df.select("product_id").distinct().count()
views = df.filter(df["event_type"] == "view").count()
rows = df.count()

transformation_time = (time() - transformation_time) / 60
print(f"The transformation of the data took {transformation_time:.2f} minutes")

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

# Cálculo del tiempo total y creación del DataFrame de tiempo
ended_at = time()
total_time = round((ended_at - started_at) / 60, 2)

schema = StructType([StructField("time", DoubleType(), True)])
time_df = spark.createDataFrame([(total_time,)], schema)

time_df.write.mode("append").jdbc(pg_url, "time_stats", properties=pg_properties)

print(f"This job took {total_time:.2f} minutes and processed {total_rows} rows")
