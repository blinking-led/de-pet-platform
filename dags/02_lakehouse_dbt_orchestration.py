from datetime import datetime, timedelta
import os
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

# Базовые параметры dbt в контейнере
DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"

default_args = {
    'owner': 'dataops_architect',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=10),
}

@dag(
    dag_id='02_lakehouse_dbt_orchestration',
    default_args=default_args,
    description='Production ELT: ClickHouse Pushdown + dbt Quality Gate (Silver to Gold)',
    schedule_interval='@hourly',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['lakehouse', 'clickhouse', 'dbt', 'gold']
)
def dbt_lakehouse_pipeline():

    # 1. Запуск инкрементальных моделей Silver (stg) и Gold (marts)
    dbt_run = BashOperator(
        task_id='dbt_run_models',
        bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
        env={
            'PATH': '/home/airflow/.local/bin:' + os.environ.get('PATH', ''),
            'CLICKHOUSE_HOST': os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
            'CLICKHOUSE_USER': os.environ.get('CLICKHOUSE_USER', 'ch_admin'),
            'CLICKHOUSE_PASSWORD': os.environ.get('CLICKHOUSE_PASSWORD', ''),
        },
        append_env=True
    )

    # 2. Data Quality Gate: проверка уникальности, null-значений и допустимых статусов
    dbt_test = BashOperator(
        task_id='dbt_test_quality_gate',
        bash_command=f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
        env={
            'PATH': '/home/airflow/.local/bin:' + os.environ.get('PATH', ''),
            'CLICKHOUSE_HOST': os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
            'CLICKHOUSE_USER': os.environ.get('CLICKHOUSE_USER', 'ch_admin'),
            'CLICKHOUSE_PASSWORD': os.environ.get('CLICKHOUSE_PASSWORD', ''),
        },
        append_env=True
    )

    # Строгий порядок выполнения (Fail-Fast паттерн)
    dbt_run >> dbt_test

dbt_lakehouse_pipeline()
