from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime


# Definir la función para extraer datos con el decorador
@task
def extract_data_from_postgres():
    # Conexión a PostgreSQL usando el nombre de la conexión definida en Airflow
    postgres_hook = PostgresHook(postgres_conn_id="tu_conexion_postgres")

    # Ejecutar una consulta SQL
    sql = "SELECT * FROM nombre_de_tu_tabla LIMIT 10;"

    # Extraer los datos
    result = postgres_hook.get_records(sql)

    # Imprimir los resultados
    for row in result:
        print(row)


# Definir el DAG
with DAG(
    "extract_data_postgres", start_date=datetime(2025, 1, 8), schedule_interval=None
) as dag:

    # Llamar a la función decorada para extraer los datos
    extract_data_from_postgres()
