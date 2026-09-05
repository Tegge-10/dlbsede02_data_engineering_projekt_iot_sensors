"""
data_pipeline.py

Batch loader for IoT sensor telemetry data.

Reads sensor measurements from a CSV file, validates and converts data types,
splits the data into fixed-size batches, and inserts each batch into MongoDB.
Every batch attempt (success or failure) is logged into a dedicated
'ingestion_log' collection so that failed batches can be identified and
retried without re-processing data that already loaded successfully.

Each measurement document is given a deterministic _id built from its
device id and timestamp. This makes inserts idempotent: re-running the
loader (e.g. after a crash, or a manual re-run during testing) will not
create duplicate measurements, since MongoDB rejects a second insert with
an _id that already exists.
"""

import os
import sys
import glob
import datetime
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import PyMongoError, BulkWriteError

# --- Configuration -----------------------------------------------------

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DATABASE_NAME = "sensor_data"
MEASUREMENTS_COLLECTION = "measurements"
INGESTION_LOG_COLLECTION = "ingestion_log"

DATA_DIR = "/app/data"          # mounted via bind mount in docker-compose.yml
BATCH_SIZE = 10000              # number of rows loaded into MongoDB per batch

# Columns and their expected types, based on the sample dataset structure
BOOLEAN_COLUMNS = ["motion", "light"]
FLOAT_COLUMNS = ["co", "humidity", "lpg", "smoke", "temp"]


def utcnow() -> datetime.datetime:
    """Return the current UTC time as a timezone-aware datetime object."""
    return datetime.datetime.now(datetime.timezone.utc)


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


def build_document_id(record: dict) -> str:
    """
    Build a deterministic, unique _id from device + timestamp.
    Using this as the MongoDB _id makes inserts idempotent: loading the
    same source row twice will hit a duplicate key error on the second
    attempt instead of creating a duplicate measurement.
    """
    ts_value = record["ts"]
    ts_iso = ts_value.isoformat() if hasattr(ts_value, "isoformat") else str(ts_value)
    return f"{record['device']}_{ts_iso}"


def batch_dataframe(df: pd.DataFrame, batch_size: int):
    """Yield successive (batch_index, DataFrame chunk) pairs."""
    for i in range(0, len(df), batch_size):
        yield i // batch_size, df.iloc[i : i + batch_size]


def load_batches(df: pd.DataFrame, measurements_col, ingestion_log_col, source_file: str):
    """Insert data batch by batch, logging the outcome of every batch."""
    total_batches = (len(df) + BATCH_SIZE - 1) // BATCH_SIZE
    successful_batches = 0
    failed_batches = 0
    total_inserted = 0
    total_duplicates = 0

    for batch_index, batch_df in batch_dataframe(df, BATCH_SIZE):
        records = batch_df.to_dict("records")
        for record in records:
            record["_id"] = build_document_id(record)

        log_entry = {
            "source_file": source_file,
            "batch_index": batch_index,
            "batch_size": len(records),
            "started_at": utcnow(),
        }

        try:
            # ordered=False lets MongoDB skip only the duplicate documents
            # within a batch and still insert every valid, new one
            result = measurements_col.insert_many(records, ordered=False)
            inserted_count = len(result.inserted_ids)
            duplicate_count = len(records) - inserted_count

            log_entry["status"] = "success"
            log_entry["inserted"] = inserted_count
            log_entry["duplicates_skipped"] = duplicate_count
            log_entry["finished_at"] = utcnow()

            successful_batches += 1
            total_inserted += inserted_count
            total_duplicates += duplicate_count

        except BulkWriteError as bwe:
            # Some documents in this batch already existed (duplicate _id).
            # This is expected on a re-run and is not a real failure:
            # every document that was new has still been inserted.
            write_errors = bwe.details.get("writeErrors", [])
            duplicate_count = sum(1 for e in write_errors if e.get("code") == 11000)
            other_errors = [e for e in write_errors if e.get("code") != 11000]
            inserted_count = len(records) - len(write_errors)

            if other_errors:
                log_entry["status"] = "failed"
                log_entry["error"] = str(other_errors)
                failed_batches += 1
            else:
                log_entry["status"] = "success"
                failed_batches += 0
                successful_batches += 1

            log_entry["inserted"] = inserted_count
            log_entry["duplicates_skipped"] = duplicate_count
            log_entry["finished_at"] = utcnow()

            total_inserted += inserted_count
            total_duplicates += duplicate_count

        except PyMongoError as e:
            log_entry["status"] = "failed"
            log_entry["error"] = str(e)
            log_entry["finished_at"] = utcnow()
            failed_batches += 1
        finally:
            ingestion_log_col.insert_one(log_entry)

        print(
            f"Batch {batch_index + 1}/{total_batches}: {log_entry['status']} "
            f"(inserted={log_entry.get('inserted', 0)}, "
            f"duplicates_skipped={log_entry.get('duplicates_skipped', 0)})"
        )

    return successful_batches, failed_batches, total_batches, total_inserted, total_duplicates


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

        successful, failed, total, inserted, duplicates = load_batches(
            df, measurements_col, ingestion_log_col, os.path.basename(csv_path)
        )

        print(
            f"\nDone. {successful}/{total} batches succeeded, "
            f"{failed}/{total} batches failed.\n"
            f"Total new documents inserted: {inserted}\n"
            f"Total duplicates skipped (already loaded previously): {duplicates}"
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