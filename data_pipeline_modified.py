from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from utils import extract, transform, load

default_args = {
    "owner": "your_username",
    "email": "your_email",
    "email_on_failure": True,
    "depends_on_past": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    'weather_pipeline',
    default_args=default_args,
    description='A simple weather data pipeline',
    tags=['weather', 'etl', 'pipeline', 'data engineering'],
) as dag:
    extract_task = PythonOperator(
        task_id = 'extract_weather_data',
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id = 'transform_weather_data',
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id = 'load_weather_data',
        python_callable=load
    )

    extract_task >> transform_task >> load_task