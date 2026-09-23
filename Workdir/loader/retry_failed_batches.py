"""
retry_failed_batches.py

Standalone script to retry batches that previously failed during a
data_pipeline.py run.

This script is intentionally separate from the main loader: it can be run
on demand (e.g. by an operator, or on a schedule) without touching or
re-running the main ingestion pipeline. It works purely off the
'ingestion_log' collection that the loader already writes to, re-reads
only the rows belonging to failed batches from the original source file,
and re-attempts the insert.

Because every measurement document uses a deterministic _id (device +
timestamp), retries are safe even if some documents in a "failed" batch
had actually been inserted before the failure occurred: MongoDB will
simply reject the ones that already exist and insert only the missing
ones, exactly like the main loader already does.

Usage:
    docker compose run --rm loader python retry_failed_batches.py
"""

import os
import sys
import glob
import datetime
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import PyMongoError, BulkWriteError

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DATABASE_NAME = "sensor_data"
MEASUREMENTS_COLLECTION = "measurements"
INGESTION_LOG_COLLECTION = "ingestion_log"

DATA_DIR = "/app/data"
BATCH_SIZE = 10000

BOOLEAN_COLUMNS = ["motion", "light"]
FLOAT_COLUMNS = ["co", "humidity", "lpg", "smoke", "temp"]


def utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def find_csv_file(data_dir: str) -> str:
    csv_files = glob.glob(os.path.join(data_dir, "*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV file found in {data_dir}")
    return csv_files[0]


def load_and_clean_data(csv_path: str) -> pd.DataFrame:
    """
    Re-applies the exact same cleaning steps as the main loader, so that
    row order and batch boundaries line up identically with the original
    run. Only rows usable as measurements are returned; this mirrors the
    'valid_df' half of the main loader's load_and_clean_data function.
    """
    df = pd.read_csv(csv_path)
    df["ts"] = pd.to_datetime(df["ts"], unit="s", errors="coerce")

    for col in BOOLEAN_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype(bool)
    for col in FLOAT_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["ts", "device"])
    return df.reset_index(drop=True)


def build_document_id(record: dict) -> str:
    ts_value = record["ts"]
    ts_iso = ts_value.isoformat() if hasattr(ts_value, "isoformat") else str(ts_value)
    return f"{record['device']}_{ts_iso}"


def get_failed_batches(ingestion_log_col, source_file: str):
    """
    Return the sorted list of distinct batch_index values that are
    currently marked as 'failed' for the given source file, considering
    only the most recent log entry per batch (a previous retry attempt
    may have already turned a failure into a success).
    """
    pipeline = [
        {"$match": {"source_file": source_file}},
        {"$sort": {"batch_index": 1, "started_at": -1}},
        {"$group": {
            "_id": "$batch_index",
            "latest_status": {"$first": "$status"},
        }},
        {"$match": {"latest_status": "failed"}},
        {"$sort": {"_id": 1}},
    ]
    return [doc["_id"] for doc in ingestion_log_col.aggregate(pipeline)]


def retry_batch(batch_index: int, df: pd.DataFrame, measurements_col, ingestion_log_col, source_file: str):
    """Re-attempt inserting exactly the rows belonging to one failed batch."""
    start = batch_index * BATCH_SIZE
    end = start + BATCH_SIZE
    batch_df = df.iloc[start:end]

    if batch_df.empty:
        print(f"Batch {batch_index}: no rows found at this position in the "
              f"current source file, skipping (source file may have changed).")
        return

    records = batch_df.to_dict("records")
    for record in records:
        record["_id"] = build_document_id(record)

    log_entry = {
        "source_file": source_file,
        "batch_index": batch_index,
        "batch_size": len(records),
        "started_at": utcnow(),
        "retry_of": "failed",
    }

    try:
        result = measurements_col.insert_many(records, ordered=False)
        inserted_count = len(result.inserted_ids)
        duplicate_count = len(records) - inserted_count
        log_entry["status"] = "retry_success"
        log_entry["inserted"] = inserted_count
        log_entry["duplicates_skipped"] = duplicate_count

    except BulkWriteError as bwe:
        write_errors = bwe.details.get("writeErrors", [])
        duplicate_count = sum(1 for e in write_errors if e.get("code") == 11000)
        other_errors = [e for e in write_errors if e.get("code") != 11000]
        inserted_count = len(records) - len(write_errors)

        log_entry["inserted"] = inserted_count
        log_entry["duplicates_skipped"] = duplicate_count
        if other_errors:
            log_entry["status"] = "failed"
            log_entry["error"] = str(other_errors)
        else:
            log_entry["status"] = "retry_success"

    except PyMongoError as e:
        log_entry["status"] = "failed"
        log_entry["error"] = str(e)

    finally:
        log_entry["finished_at"] = utcnow()
        ingestion_log_col.insert_one(log_entry)

    print(f"Retry batch {batch_index}: {log_entry['status']} "
          f"(inserted={log_entry.get('inserted', 0)}, "
          f"duplicates_skipped={log_entry.get('duplicates_skipped', 0)})")


def main():
    print(f"Connecting to MongoDB at {MONGO_URI} ...")
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    measurements_col = db[MEASUREMENTS_COLLECTION]
    ingestion_log_col = db[INGESTION_LOG_COLLECTION]

    try:
        csv_path = find_csv_file(DATA_DIR)
        source_file = os.path.basename(csv_path)

        failed_batches = get_failed_batches(ingestion_log_col, source_file)
        if not failed_batches:
            print("No failed batches found. Nothing to retry.")
            return

        print(f"Found {len(failed_batches)} failed batch(es) for "
              f"'{source_file}': {failed_batches}")

        df = load_and_clean_data(csv_path)

        for batch_index in failed_batches:
            retry_batch(batch_index, df, measurements_col, ingestion_log_col, source_file)

    except Exception as e:
        print(f"Fatal error during retry execution: {e}")
        sys.exit(1)
    finally:
        client.close()


if __name__ == "__main__":
    main()