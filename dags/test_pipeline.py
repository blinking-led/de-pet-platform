from datetime import datetime, timedelta
from airflow.decorators import dag, task


default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


@dag(
    dag_id='01_test_gitops_pipeline',
    default_args=default_args,
    description='Smoke-test DAG для проверки доставки через GitHub Actions',
    schedule_interval=None, 
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['test', 'gitops'],
)
def test_pipeline():

    @task()
    def extract():
        print(">>> Извлечение данных из источника...")
        return {"batch_id": 101, "records_count": 1000}

    @task()
    def transform(data: dict):
        print(f">>> Обработка пакета {data['batch_id']}...")
        processed_count = data['records_count']
        return f"Успешно обработано {processed_count} записей"

    @task()
    def load(status: str):
        print(f">>> Финал: {status}")

    raw_data = extract()
    transformed_data = transform(raw_data)
    load(transformed_data)


test_pipeline()
