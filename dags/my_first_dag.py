from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

# Define DAG
with DAG(
    dag_id="demo2",
    start_date=datetime(2025, 1, 6),  # Fecha en el pasado
    schedule="*/1 * * * * ",
    catchup=False,  # Evita ejecutar tareas pendientes para fechas pasadas
    tags=["demo"],  # Añade etiquetas descriptivas
) as dag:

    # Tarea: Imprimir un mensaje
    print_hello = BashOperator(
        task_id="print_hello", bash_command="echo 'Hello, Airflow!'"
    )

    # Tarea: Escribir en un archivo
    @task
    def write_to_file():
        file_path = "test.txt"  # Ruta en un directorio temporal
        with open(file_path, "a") as file:
            file.write("Hello, Airflow!\n")
        return file_path  # Devuelve la ruta para futuros pasos (si es necesario)

    # Definir dependencias
    print_hello >> write_to_file()
