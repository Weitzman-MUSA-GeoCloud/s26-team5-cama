"""
Cloud Function to prepare SEPTA Stations data for BigQuery.

This function reads the raw SEPTA Stations CSV from Cloud Storage,
converts it to Parquet format, and writes to the prepared data bucket.

Usage:
    Deploy as a Cloud Function named "prepare-septa"
"""

import functions_framework
import os
import pandas as pd
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()


@functions_framework.http
def prepare_septa(request):
    """HTTP Cloud Function to prepare SEPTA Stations data.

    Reads the raw CSV from Cloud Storage, converts to Parquet,
    and uploads to the prepared data bucket.

    Args:
        request: The HTTP request object.

    Returns:
        A response indicating success or failure.
    """
    try:
        raw_data_bucket = os.getenv("RAW_DATA_BUCKET", "musa5090s26-team5-raw_data")
        prepared_data_bucket = os.getenv(
            "PREPARED_DATA_BUCKET", "musa5090s26-team5-prepared_data"
        )

        storage_client = storage.Client()
        raw_blob = storage_client.bucket(raw_data_bucket).blob(
            "septa/data.csv"
        )

        # Download CSV to a temp file.
        temp_csv = "/tmp/septa.csv"
        temp_parquet = "/tmp/septa.parquet"

        print("Downloading raw SEPTA Stations CSV.")
        raw_blob.download_to_filename(temp_csv)

        # Read CSV and convert to Parquet.
        print("Processing CSV into Parquet.")
        df = pd.read_csv(temp_csv)

        # Lowercase column names.
        df.columns = [c.lower() for c in df.columns]

        print(f"Processing {len(df)} records.")

        # Write Parquet to temp file.
        print("Writing Parquet file.")
        df.to_parquet(temp_parquet, index=False)

        # Upload to prepared data bucket.
        prepared_blob = storage_client.bucket(prepared_data_bucket).blob(
            "septa/data.parquet"
        )
        print("Uploading to Cloud Storage.")
        prepared_blob.upload_from_filename(temp_parquet)

        print(f"Uploaded to gs://{prepared_data_bucket}/septa/data.parquet")

        return (
            f"Successfully prepared {len(df)} SEPTA stations records as Parquet.",
            200,
        )

    except Exception as e:
        print(f"Error: {e}.")
        return (f"Error: {e}.", 500)
