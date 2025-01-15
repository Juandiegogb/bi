"""
Esta es una ETL con Python y Pyspark
"""


from pyspark.sql import SparkSession
from config import mssql, pg
from pyspark.sql.types import StructType, StructField, StringType, LongType



spark: SparkSession = (
    SparkSession.builder.appName("ETL")
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
pg_properties: dict = {
    "user": pg["user"],
    "password": pg["pwd"],
    "driver": "org.postgresql.Driver",
}



query = "(SELECT TOP 100 * FROM behavior_october) AS temp_table"

df = spark.read.jdbc(
    mssql_url, table=query, properties=mssql_properties
)



total_rows = df.count()



# Crear un RDD a partir de los datos
stats_rdd = spark.sparkContext.parallelize([("rows", df.count())])

# Convertir el RDD en un DataFrame
stats_columns = ["stat", "value"]
stats_df = stats_rdd.toDF(stats_columns)

# Mostrar el DataFrame
stats_df.show()

df.show()

stats_df.write.mode("overwrite").jdbc(pg_url, table="stats", properties=pg_properties)