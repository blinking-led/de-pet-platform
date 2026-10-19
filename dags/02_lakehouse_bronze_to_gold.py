from datetime import datetime, timedelta
import io
import os
from typing import List, Dict, Any

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg2
import boto3
from botocore.config import Config
import pandas as pd
import pyarrow.parquet as pq

S3_ENDPOINT_URL = "http://minio:9000"
S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin_secure_pass"
S3_BUCKET_NAME = "lakehouse"

POSTGRES_CONN_ID = "postgres_default"
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "airflow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow_secure_pass")
POSTGRES_DB = os.getenv("POSTGRES_DB", "airflow")

def get_pg_connection():
    """Fallback фабрика: использует Airflow Hook либо прямое соединение с PostgreSQL."""
    try:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        return hook.get_conn()
    except Exception:
        return psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            dbname=POSTGRES_DB
        )

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id='02_lakehouse_bronze_to_gold',
    default_args=default_args,
    schedule_interval='@hourly',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['lakehouse', 'bronze', 'gold', 'retail']
)
def lakehouse_etl():

    @task
    def init_gold_schema() -> None:
        """DDL инициализация схемы и витрины с поддержкой атомарного UPSERT."""
        # hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        ddl_query = """
        CREATE SCHEMA IF NOT EXISTS gold;

        CREATE TABLE IF NOT EXISTS gold.mart_hourly_sales (
            window_start TIMESTAMP NOT NULL,
            store_id INT NOT NULL,
            category VARCHAR(64) NOT NULL,
            brand VARCHAR(64) NOT NULL,
            total_orders INT NOT NULL,
            successful_orders INT NOT NULL,
            total_revenue NUMERIC(14, 2) NOT NULL,
            total_items_sold INT NOT NULL,
            avg_order_value NUMERIC(14, 2) NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (window_start, store_id, category, brand)
        );
        """
        # hook.run(ddl_query)
        conn = get_pg_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(ddl_query)
            conn.commit()
        finally:
            conn.close()

    @task
    def extract_and_transform_silver(**context) -> List[Dict[str, Any]]:
        """
        Чтение Parquet из MinIO за расчетный час, дедупликация (Silver)
        и расчет агрегированных метрик (Gold).
        """
        logical_date = context.get('logical_date', datetime.utcnow())
        
        # Hive partition path
        prefix = f"bronze/transactions/year={logical_date.year}/month={logical_date.month:02d}/day={logical_date.day:02d}/hour={logical_date.hour:02d}/"

        s3_client = boto3.client(
            's3',
            endpoint_url=S3_ENDPOINT_URL,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
            config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}),
            region_name='us-east-1'
        )

        # Поиск файлов в партиции
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix)
        objects = response.get('Contents', [])

        # Fallback для ручного триггера: если за текущий час объектов мало, берем последние доступные объекты
        if not objects:
            fallback_resp = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix="bronze/transactions/", MaxKeys=20)
            objects = fallback_resp.get('Contents', [])

        if not objects:
            return []

        dataframes = []
        for obj in objects:
            if not obj['Key'].endswith('.parquet'):
                continue
            file_obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=obj['Key'])
            parquet_bytes = file_obj['Body'].read()
            table = pq.read_table(io.BytesIO(parquet_bytes))
            dataframes.append(table.to_pandas())

        if not dataframes:
            return []

        # Объединение и этап Silver (Очистка + Дедупликация)
        df_silver = pd.concat(dataframes, ignore_index=True)
        df_silver.drop_duplicates(subset=['transaction_id'], keep='last', inplace=True)

        # Распаковка структуры item
        df_silver['category'] = df_silver['item'].apply(lambda x: x['category'])
        df_silver['brand'] = df_silver['item'].apply(lambda x: x['brand'])
        df_silver['price'] = df_silver['item'].apply(lambda x: x['price'])
        df_silver['quantity'] = df_silver['item'].apply(lambda x: x['quantity'])
        df_silver['total_amount'] = df_silver['item'].apply(lambda x: x['total_amount'])

        # Временное окно (округление timestamp_ms до часа)
        df_silver['window_start'] = pd.to_datetime(df_silver['timestamp_ms'], unit='ms').dt.floor('h')

        # Флаг успешности заказа
        df_silver['is_success'] = df_silver['status'] == 'CONFIRMED'

        # Этап Gold (Агрегация в разрезе PK: window_start, store_id, category, brand)
        grouped = df_silver.groupby(['window_start', 'store_id', 'category', 'brand'])

        gold_records = []
        for (w_start, store_id, cat, brand), group in grouped:
            success_subset = group[group['is_success']]
            
            total_orders = int(len(group))
            successful_orders = int(len(success_subset))
            total_revenue = float(round(success_subset['total_amount'].sum(), 2))
            total_items = int(success_subset['quantity'].sum())
            aov = float(round(total_revenue / successful_orders, 2)) if successful_orders > 0 else 0.0

            gold_records.append({
                'window_start': w_start.strftime('%Y-%m-%d %H:%M:%S'),
                'store_id': int(store_id),
                'category': str(cat),
                'brand': str(brand),
                'total_orders': total_orders,
                'successful_orders': successful_orders,
                'total_revenue': total_revenue,
                'total_items_sold': total_items,
                'avg_order_value': aov
            })

        return gold_records

    @task
    def load_gold_mart(records: List[Dict[str, Any]]) -> None:
        """Идемпотентная загрузка в PostgreSQL (UPSERT)."""
        if not records:
            return

        # hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        upsert_query = """
        INSERT INTO gold.mart_hourly_sales (
            window_start, store_id, category, brand,
            total_orders, successful_orders, total_revenue,
            total_items_sold, avg_order_value, updated_at
        ) VALUES (
            %(window_start)s, %(store_id)s, %(category)s, %(brand)s,
            %(total_orders)s, %(successful_orders)s, %(total_revenue)s,
            %(total_items_sold)s, %(avg_order_value)s, CURRENT_TIMESTAMP
        )
        ON CONFLICT (window_start, store_id, category, brand)
        DO UPDATE SET
            total_orders = EXCLUDED.total_orders,
            successful_orders = EXCLUDED.successful_orders,
            total_revenue = EXCLUDED.total_revenue,
            total_items_sold = EXCLUDED.total_items_sold,
            avg_order_value = EXCLUDED.avg_order_value,
            updated_at = CURRENT_TIMESTAMP;
        """

        # conn = hook.get_conn()
        conn = get_pg_connection()
        cursor = conn.cursor()
        try:
            cursor.executemany(upsert_query, records)
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    # Порядок выполнения (DAG Task Flow)
    schema_ready = init_gold_schema()
    mart_data = extract_and_transform_silver()
    load_op = load_gold_mart(mart_data)

    schema_ready >> mart_data >> load_op

lakehouse_etl()
