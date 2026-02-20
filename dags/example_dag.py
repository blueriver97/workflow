from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def print_hello():
    print("Hello from Airflow!")


with DAG(
    "example_dag",
    default_args=default_args,
    description="A simple tutorial DAG",
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id="print_hello",
        python_callable=print_hello,
    )

    t1
