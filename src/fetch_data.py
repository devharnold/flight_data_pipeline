
from pathlib import Path

import pandas as pd


# Project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATASETS_DIR = PROJECT_ROOT / "datasets"
PARQUET_DIR = PROJECT_ROOT / "data" / "parquet"

# Dataset name -> source CSV, output Parquet file, and S3 key.
DATASET_NAMES = [
    "airports",
    "airport-comments",
    "airport-frequencies",
    "countries",
    "navaids",
    "regions",
    "runways",
]

DATASETS = {
    name: {
        "file": DATASETS_DIR / f"{name}.csv",
        "parquet_file": PARQUET_DIR / f"{name}.parquet",
        "s3_key": f"{name}/raw/{name}.parquet",
    }
    for name in DATASET_NAMES
}


def get_dataset_config(dataset: str) -> dict:
    """Return configuration for a known dataset."""
    if dataset not in DATASETS:
        raise ValueError(
            f"Unknown dataset: {dataset}. "
            f"Available datasets: {', '.join(DATASETS)}"
        )

    return DATASETS[dataset]


def extract_data_from_csv(dataset: str) -> pd.DataFrame:
    """Read a dataset CSV file into a pandas DataFrame."""
    config = get_dataset_config(dataset)
    csv_path = config["file"]

    if not csv_path.is_file():
        raise FileNotFoundError(
            f"CSV file not found: {csv_path}. "
            "Check the datasets directory and Docker volume mounts."
        )

    df = pd.read_csv(csv_path)

    print(f"Extracted {len(df)} rows from {csv_path}")

    return df


def transform_data_from_csv(dataset: str, df: pd.DataFrame) -> str:
    # Deduplicate a DataFrame, save it as Parquet, and return its path.
    config = get_dataset_config(dataset)
    parquet_path = config["parquet_file"]

    before = len(df)

    transformed_df = df.drop_duplicates()

    after = len(transformed_df)

    parquet_path.parent.mkdir(parents=True, exist_ok=True)

    transformed_df.to_parquet(
        parquet_path,
        engine="pyarrow",
        compression="snappy",
        index=False,
    )

    print(f"Transformation complete for {dataset}")
    print(f"Rows before deduplication: {before}")
    print(f"Rows after deduplication: {after}")
    print(f"Parquet file: {parquet_path}")

    return str(parquet_path)


if __name__ == "__main__":
    for dataset in DATASET_NAMES:
        dataframe = extract_data_from_csv(dataset)
        transform_data_from_csv(dataset, dataframe)
