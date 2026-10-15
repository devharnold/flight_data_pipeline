import os
import pendulum

from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue_crawler import (
    GlueCrawlerOperator,
)

from flight_data_pipeline.src.fetch_data import (
    DATASETS,
    DATASET_NAMES,
    extract_data_from_csv,
    transform_data_from_csv,
)
# Config

local_tz = pendulum.timezone("Africa/Nairobi")

DEST_BUCKET = os.getenv("DEST_BUCKET")
AWS_CONN_ID = os.getenv("AWS_CONN_ID", "aws_default")
DATABASE_NAME = os.getenv("DATABASE_NAME")
S3_TARGET_PATH = os.getenv("S3_TARGET_PATH")


def get_s3_key(dataset: str) -> str:
    """
    Add the configured S3 target prefix to the dataset's S3 key.

    Example:
        S3_TARGET_PATH = s3://my-bucket/flight-data
        dataset key    = airports/raw/airports.parquet
        final key      = flight-data/airports/raw/airports.parquet
    """
    if not DEST_BUCKET:
        raise ValueError("DEST_BUCKET is not configured.")

    if not S3_TARGET_PATH:
        raise ValueError("S3_TARGET_PATH is not configured.")

    if not S3_TARGET_PATH.startswith("s3://"):
        raise ValueError(
            "S3_TARGET_PATH must be a full S3 URI, "
            "such as s3://my-bucket/flight-data"
        )

    bucket_and_prefix = S3_TARGET_PATH[5:].strip("/")
    target_bucket, separator, prefix = bucket_and_prefix.partition("/")

    if target_bucket != DEST_BUCKET:
        raise ValueError(
            f"S3_TARGET_PATH bucket '{target_bucket}' does not match "
            f"DEST_BUCKET '{DEST_BUCKET}'."
        )

    dataset_key = DATASETS[dataset]["s3_key"]

    if separator and prefix:
        return f"{prefix}/{dataset_key}"

    return dataset_key


# DAG
@dag(
    dag_id="sky_stream_open_data_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz=local_tz),
    catchup=False,
    default_args={
        "owner": "Sky Stream ELT",
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=3),
    },
    tags=["ELT", "Open Flight Data"],
)
def start_pipeline():

    @task
    def process_and_upload_dataset(dataset: str) -> str:
        # Extract, transform, and upload one dataset
        if dataset not in DATASETS:
            raise ValueError(f"Unknown dataset: {dataset}")

        # Extract CSV data into a DataFrame.
        dataframe = extract_data_from_csv(dataset)

        # Deduplicate and write the Parquet file.
        #    The function returns the local Parquet file path.
        parquet_path = transform_data_from_csv(dataset, dataframe)

        # 3. Upload the Parquet file to S3.
        s3_key = get_s3_key(dataset)

        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

        s3_hook.load_file(
            filename=parquet_path,
            key=s3_key,
            bucket_name=DEST_BUCKET,
            replace=True,
        )

        print(
            f"Successfully uploaded {dataset} to "
            f"s3://{DEST_BUCKET}/{s3_key}"
        )

        return s3_key

    # Configure the Glue crawler.
    if not DATABASE_NAME:
        raise ValueError("DATABASE_NAME is not configured.")

    if not S3_TARGET_PATH:
        raise ValueError("S3_TARGET_PATH is not configured.")

    update_catalog = GlueCrawlerOperator(
        task_id="run_glue_crawler",
        config={
            "Name": "skystream_data-crawler",
            "Role": "GlueCrawlerRole",
            "DatabaseName": DATABASE_NAME,
            "Targets": {
                "S3Targets": [
                    {
                        "Path": S3_TARGET_PATH.rstrip("/") + "/",
                    }
                ]
            },
        },
        aws_conn_id=AWS_CONN_ID,
    )

    # Create one processing/upload task per dataset.
    upload_tasks = []

    for dataset in DATASET_NAMES:
        upload_task = process_and_upload_dataset.override(
            task_id=f"process_upload_{dataset.replace('-', '_')}"
        )(dataset)

        upload_tasks.append(upload_task)

    # Run the crawler only after every upload succeeds.
    for upload_task in upload_tasks:
        upload_task >> update_catalog


start_pipeline()
