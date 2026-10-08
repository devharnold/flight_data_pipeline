import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATASETS_DIR = PROJECT_ROOT / "datasets"
PARQUET_DIR = PROJECT_ROOT / "data" / "parquet"

# Skip formatting data type: Lets be evil and trust what's there, We'll eventually transform it anyway
AIRPORT_DATA = "dataset/airports.csv"
AIRPORT_COMMENTS_DATA = "dataset/airport_comments.csv"
AIRPORT_FREQUENCIES_DATA = "dataset/airport_frequencies.csv"
COUNTRIES_DATA = "dataset/countries.csv"
NAVAIDS_DATA = "dataset/navaids.csv"
REGIONS_DATA = "dataset/regions.csv"
RUNWAYS_DATA = "dataset/runways.csv"

DATASETS = {
        "airports": {
            "file": AIRPORT_DATA,
            "parquet_file": "",
            "s3_Key": "airport/raw/airport.parquet",
            },
        "airport_comments": {
                "file": AIRPORT_COMMENTS_DATA,
                "parquet_file": "",
                "s3_Key": "airport_comments/raw/airport_comments.parquet",
                },
        "airport_frequencies": {
            "file": AIRPORT_FREQUENCIES_DATA,
            "parquet_file": "",
            "s3_Key": "airport_frequencies/raw/airport_frequencies.parquet",
            },
        "countries": {
            "file": COUNTRIES_DATA,
            "parquet_file": "/opt/airflow/...",
            "s3_Key": "countries_data/raw/countries.parquet",
            },
        "navaids": {
            "file": NAVAIDS_DATA,
            "parquet_file": "",
            "s3_Key": "navaids/raw/navaids.parquet",
            },
        "regions": {
            "file": REGIONS_DATA,
            "parquet_file": "",
            "s3_Key": "regions/raw/regions.parquet",
            },
        "runways": {
            "file": RUNWAYS_DATA,
            "parquet_file": "",
            "s3_Key": "runways/raw/runways.parquet",
            }


def extract_data_from_csv(dataset: str) -> pd.DataFrame:
    # Read a csv file into a Data Frame
    config = DATASETS[dataset]

    df = pd.read_csv(config["file"])
    print(f"Extracted {len(df)} rows from {config['file']}")

    return df

def transform_data_from_csv(dataset: str, df: pd.DataFrame) -> str:
    df = pd.read_json(data)
    df = df.drop_duplicates()

    config = DATASETS[dataset]

    df = pd.read_csv(config["file"])
    before = len(df)

    df = df.drop_duplicates()
    after = len(df)

    config["parquet_file"].parent.mkdir(
            parents=True,
            exists_ok=True
        )

    df.to_parquet(
            config["parquet_file"],
            engine="pyarrow",
            compression="snappy",
            index=False,
            )

    print(f"Transformation complete for {dataset}")
    print(f"Rows before: {before}")
    print(f"Rows after: {after}")
    print(f"Parquet file: {config['parquet_file']}")

    return str(config["parquet_file"])

def get_s3_key(dataset: str) -> str:
    # Returns the S3 destination key for a dataset
    return DATASETS[dataset]["s3_Key"]

