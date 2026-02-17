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

TARGET_ITEM_NAME = "AK-47 | Redline (Field-Tested)"

# Функция Extract в ELT 
def get_pricempire_free_price(**context):

    import requests
    import logging

    logger = logging.getLogger(__name__)

    api_token = Variable.get("pricempire_api_token")
    ts = context['ts_nodash']
    filename = f"prices_{ts}.json"
    filepath = os.path.join(DATA_LAKE_PATH, filename)

    # Для процессинга ранее полученных JSON
    if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
        logger.warning(f"Файл {filepath} уже существует! Скачивание пропущено для защиты исторических данных.")
        return
    
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

def process_pricempire_data(**context):

    import json
    import sqlite3
    import logging

    # Для meta от skins
    PROVIDER_DEFAULTS = {
        "skins": {"currency": "USD", "rate": 1.0},
    }

    # Обработчит JSON с бизнес-логикой
    def parse_entry(entry):
        meta = entry.get('meta') or {}
        source = entry.get('provider_key')
        defaults = PROVIDER_DEFAULTS.get(source, {})
        
        # Считаем что ключ price это цена в USD
        price_usd = entry.get('price')
        
        # Если в meta есть дополнительная информация относительно валют и курсов
        # , то достём её - иначе берём предопределённые значения для USD-площадок
        currency = meta.get('original_currency') or defaults.get('currency')
        rate = meta.get('rate') or defaults.get('rate')
        orig_price = meta.get('original_price')


        if currency == "USD" and orig_price is None:
            orig_price = price_usd
            
        return (
            TARGET_ITEM_NAME,
            source,
            price_usd,
            orig_price,
            currency,
            rate,
            entry.get('count'),
            entry.get('avg_7'),
            entry.get('avg_30'),
            entry.get('median_7'),
            entry.get('median_30'),
            entry.get('updated_at'),
            context['ts']
        )

    logger = logging.getLogger(__name__)
    
    ts = context['ts_nodash']
    filename = f"prices_{ts}.json"
    filepath = os.path.join(DATA_LAKE_PATH, filename)
    
    db_path = os.path.join(DATA_LAKE_PATH, "skins_analytics.db")

    if not os.path.exists(filepath):
        logger.warning(f"Файл {filepath} не найден. Процессинг остановлен.")
        return

    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Поиск нужного скина
    skin_data = next((item for item in data if item.get("market_hash_name") == TARGET_ITEM_NAME), None)

    if not skin_data:
        logger.warning(f"Скин {TARGET_ITEM_NAME} не найден в файле {filepath}.")
        return
    
    # Подключение к БД
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Создаём таблицу для первого раза, далее будет игнорироваться
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS skin_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                skin_name TEXT,
                source TEXT,
                price INTEGER,
                original_price INTEGER,
                original_currency TEXT,
                exchange_rate REAL,
                count INTEGER,
                avg_7 INTEGER,
                avg_30 INTEGER,
                median_7 INTEGER,
                median_30 INTEGER,
                updated_at TEXT,
                collected_at TEXT
            )
        """)

        # Удаляем старые данные на случай перезапуска процессинга 
        cursor.execute("DELETE FROM skin_prices WHERE collected_at = ?", (context['ts'],))
        
        prices_list = skin_data.get('prices', [])

        for entry in prices_list:
            # Обрабатываем данные - Transform
            row_data = parse_entry(entry)
            
            cursor.execute("""
                INSERT INTO skin_prices (
                    skin_name, source, price, 
                    original_price, original_currency, exchange_rate, 
                    count, avg_7, avg_30, median_7, median_30, 
                    updated_at, collected_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, row_data)

        conn.commit()
        logger.info(f"Данные по {TARGET_ITEM_NAME} успешно сохранены в БД.")
    
    except Exception as e:
        logger.error(f"Ошибка при записи в БД: {e}")
        conn.rollback()
        raise e
    
    finally:
        conn.close()

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
    tags=['free_prices', 'extract', 'transform', 'load']
) as dag:

    extract_task = PythonOperator(
        task_id='extract_free_prices',
        python_callable=get_pricempire_free_price,
        provide_context=True
    )

    process_task = PythonOperator(
        task_id='process_skin_data',
        python_callable=process_pricempire_data,
        provide_context=True
    )

    extract_task >> process_task