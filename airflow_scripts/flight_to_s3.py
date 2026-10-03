from pathlib import Path
import pendulum
from datetime import datetime

from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

local_tz = pendulum.timezone("Africa/Nairobi")

from src.fetch_data import extract_data_from_csv, transform_data_from_csv

@dag(
        dag_id="Sky Stream Open Data Pipeline",
        schedule="",
        start_date=pendulum.datetime(2026, 1, 1, tz=local_tz),
        retries=3,
        catchup=False,
        default_args={
            "owner": "Sky Stream ELT",
            "retries": 3,
            "retry_delay": timedelta(minutes=3),
            },
        tags={"ELT", "Open Flight Data"}
        )

def start_pipeline():
    @task
    def fetch_and_load_data(dataset):
        
