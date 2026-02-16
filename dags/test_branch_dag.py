from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello():
    print("Даг из ветки dev")

with DAG(
    'dev_branch_test',
    start_date=datetime(2026, 2, 16),
    schedule_interval=None,
    catchup=False
) as dag:

    task = PythonOperator(
        task_id='test',
        python_callable=hello
    )