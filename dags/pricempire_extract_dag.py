from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import timedelta
import os

# Хранилище сырых JSON-ответов от API
DATA_LAKE_PATH = "/opt/airflow/data"

# Бесплатный метод API
API_URL = "https://api.pricempire.com/v4/trader/items/prices"

# Функция Extract в ELT 
def get_pricempire_free_price(**context):

    import json
    import requests
    import logging

    logger = logging.getLogger(__name__)

    api_token = Variable.get("pricempire_api_token")
    ts = context['ts_nodash']
    filename = f"prices_{ts}.json"
    filepath = os.path.join(DATA_LAKE_PATH, filename)
    
    logger.info(f"Запуск Extract в {ts}")
    logger.info(f"Файл будет сохранен в: {filepath}")

    headers = {
        "Authorization": f"Bearer {api_token}",
        "User-Agent": "AirflowParserv1"
    }
    
    params = {
        "app_id": 730,
        "sources": "buff163,skins",
        "currency": "USD",
        "avg": True,
        "median": True,
        "inflation_threshold": -1
    }

    try:
        # Страховка на случай отсутствия папки 
        folder = os.path.dirname(filepath)
        os.makedirs(folder, exist_ok=True)

        response = requests.get(API_URL, headers=headers, params=params, timeout=60)
        
        if response.status_code == 429:
            logger.info("Ошибка 429. Обработаем в airflow retry")
            raise Exception("API Rate Limit Exceeded")
            
        response.raise_for_status()
        
        # Запись файла
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(response.text)
        
        file_size = len(response.text) / 1024 / 1024
        logger.info(f"Успешно сохранено {file_size:.2f} MB в {filepath}")
        
    except Exception as e:
        logger.info(f"Ошибка: {e}")
        raise e

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='pricempire_extract_free_prices',
    default_args=default_args,
    schedule_interval='0 * * * *',
    catchup=False,
    tags=['free_prices', 'extract']
) as dag:

    extract_task = PythonOperator(
        task_id='extract_free_prices',
        python_callable=get_pricempire_free_price,
        provide_context=True
    )