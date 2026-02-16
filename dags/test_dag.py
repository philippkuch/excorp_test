from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_identity():
    print("Даг создан из ветки dev")

with DAG(
    'git_branch_tester',
    start_date=datetime(2026, 2, 16),
    schedule_interval=None,
    catchup=False,
    tags=['testing']
) as dag:

    test_task = PythonOperator(
        task_id='test_task',
        python_callable=print_identity
    )