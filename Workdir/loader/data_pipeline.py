"""
data_pipeline.py

Batch loader for IoT sensor telemetry data.

Reads sensor measurements from a CSV file, validates and converts data types,
splits the data into fixed-size batches, and inserts each batch into MongoDB.
Every batch attempt (success or failure) is logged into a dedicated
'ingestion_log' collection so that failed batches can be identified and
retried without re-processing data that already loaded successfully.
"""

import os
import sys
import glob
import datetime
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import PyMongoError

# --- Configuration -----------------------------------------------------

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DATABASE_NAME = "sensor_data"
MEASUREMENTS_COLLECTION = "measurements"
INGESTION_LOG_COLLECTION = "ingestion_log"

DATA_DIR = "/app/data"          # mounted via bind mount in docker-compose.yml
BATCH_SIZE = 10000               # number of rows loaded into MongoDB per batch

# Columns and their expected types, based on the sample dataset structure
BOOLEAN_COLUMNS = ["motion", "light"]
FLOAT_COLUMNS = ["co", "humidity", "lpg", "smoke", "temp"]


def find_csv_file(data_dir: str) -> str:
    """Locate the CSV file to load inside the mounted data directory."""
    csv_files = glob.glob(os.path.join(data_dir, "*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV file found in {data_dir}")
    # If multiple CSVs are present, prefer one that is not the '_test' file
    non_test_files = [f for f in csv_files if "_test" not in os.path.basename(f)]
    return non_test_files[0] if non_test_files else csv_files[0]


def load_and_clean_data(csv_path: str) -> pd.DataFrame:
    """Read the CSV file and convert columns into their expected data types."""
    df = pd.read_csv(csv_path)

    # Convert Unix epoch timestamp (seconds, possibly fractional) to a proper datetime
    df["ts"] = pd.to_datetime(df["ts"], unit="s", errors="coerce")

    # Ensure boolean columns are true Python booleans, not strings like "True"/"False"
    for col in BOOLEAN_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype(bool)

    # Ensure numeric measurement columns are floats; invalid values become NaN
    for col in FLOAT_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows without a valid timestamp or device id, as these cannot be
    # meaningfully attributed to a sensor reading
    df = df.dropna(subset=["ts", "device"])

    return df


def batch_dataframe(df: pd.DataFrame, batch_size: int):
    """Yield successive (batch_index, DataFrame chunk) pairs."""
    for i in range(0, len(df), batch_size):
        yield i // batch_size, df.iloc[i : i + batch_size]


def load_batches(df: pd.DataFrame, measurements_col, ingestion_log_col, source_file: str):
    """Insert data batch by batch, logging the outcome of every batch."""
    total_batches = (len(df) + BATCH_SIZE - 1) // BATCH_SIZE
    successful_batches = 0
    failed_batches = 0

    for batch_index, batch_df in batch_dataframe(df, BATCH_SIZE):
        records = batch_df.to_dict("records")
        log_entry = {
            "source_file": source_file,
            "batch_index": batch_index,
            "batch_size": len(records),
            "started_at": datetime.datetime.utcnow(),
        }

        try:
            measurements_col.insert_many(records, ordered=False)
            log_entry["status"] = "success"
            log_entry["finished_at"] = datetime.datetime.utcnow()
            successful_batches += 1
        except PyMongoError as e:
            log_entry["status"] = "failed"
            log_entry["error"] = str(e)
            log_entry["finished_at"] = datetime.datetime.utcnow()
            failed_batches += 1
        finally:
            ingestion_log_col.insert_one(log_entry)

        print(
            f"Batch {batch_index + 1}/{total_batches}: "
            f"{log_entry['status']} ({len(records)} records)"
        )

    return successful_batches, failed_batches, total_batches


def main():
    print(f"Connecting to MongoDB at {MONGO_URI} ...")
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    measurements_col = db[MEASUREMENTS_COLLECTION]
    ingestion_log_col = db[INGESTION_LOG_COLLECTION]

    try:
        csv_path = find_csv_file(DATA_DIR)
        print(f"Loading data from {csv_path} ...")
        df = load_and_clean_data(csv_path)
        print(f"Loaded {len(df)} valid rows after cleaning.")

        successful, failed, total = load_batches(
            df, measurements_col, ingestion_log_col, os.path.basename(csv_path)
        )

        print(
            f"\nDone. {successful}/{total} batches succeeded, "
            f"{failed}/{total} batches failed."
        )

        if failed > 0:
            # Non-zero exit code signals failure, e.g. for automation or CI pipelines,
            # and indicates that failed batches remain in ingestion_log for a future retry
            sys.exit(1)

    except Exception as e:
        print(f"Fatal error during pipeline execution: {e}")
        sys.exit(1)
    finally:
        client.close()


if __name__ == "__main__":
    main()