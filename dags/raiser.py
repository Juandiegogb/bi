from airflow import DAG
from airflow.operators.python import task
from datetime import datetime


with DAG(
    "raiser",
    description="is not necesary",
    catchup=False,
    schedule="*/2 * * * *",
    start_date=datetime(2025, 1, 1),
    default_args={
        "email": ["juandiegar@gmail.com"],
        "email_on_failure": True,
    },
) as dag:

    @task(task_id="raiser_task")
    def raiser():
        raise ZeroDivisionError

    raiser()
