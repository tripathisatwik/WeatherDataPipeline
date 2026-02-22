from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta  # fixed typo: datatime → datetime

default_args = {
    "owner": "satwik",
    "retries": 2,                    # fixed typo: retires → retries
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="weather_event_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 2, 22),  # date when DAG triggers
    schedule_interval="*/10 * * * *",  # triggers every 10 minutes
    catchup=False,
) as dag:

    ingest = BashOperator(
        task_id="ingest_api",
        bash_command="python /opt/airflow/ingestion/fetch_weather.py",
    )

    load = BashOperator(
        task_id="load_to_postgres",
        bash_command="python /opt/airflow/ingestion/load_to_postgres.py",
    )

    transform = BashOperator(
        task_id="transform_clean",
        bash_command="python /opt/airflow/ingestion/transform_to_clean.py",
    )

    check = BashOperator(
        task_id="check_clean_table",
        bash_command = "python /opt/airflow/ingestion/data_quality.py",
    )

ingest >> load >> transform >> check