"""
Cloud Function to prepare Philadelphia Neighborhoods data.

This function reads the raw Philadelphia Neighborhoods Parquet from Cloud
Storage and writes it to the prepared data bucket. Since it's already in
Parquet format, this is essentially a copy operation, but also converts
DECIMAL columns to FLOAT64 to avoid BigQuery precision errors when reading
the external table.

Usage:
    Deploy as a Cloud Function named "prepare-neighborhoods"
"""

import functions_framework
import os
from google.cloud import storage
from dotenv import load_dotenv
import pyarrow.parquet as pq
import pyarrow as pa
import io

load_dotenv()


@functions_framework.http
def prepare_neighborhoods(request):
    """HTTP Cloud Function to prepare Philadelphia Neighborhoods data."""
    try:
        raw_data_bucket = os.getenv("RAW_DATA_BUCKET", "musa5090s26-team5-raw_data")
        prepared_data_bucket = os.getenv(
            "PREPARED_DATA_BUCKET", "musa5090s26-team5-prepared_data"
        )

        storage_client = storage.Client()

        # Read neighborhoods Parquet from raw data bucket.
        print("Reading neighborhoods Parquet from raw data bucket.")
        raw_blob = storage_client.bucket(raw_data_bucket).blob(
            "neighborhoods/data.parquet"
        )
        parquet_data = raw_blob.download_as_bytes()

        # Load with pyarrow and convert all decimal128 columns to float64.
        print("Converting all DECIMAL columns to FLOAT64.")
        parquet_file = pq.read_table(io.BytesIO(parquet_data))

        # Create new schema with decimal128 converted to float64.
        schema = parquet_file.schema
        new_fields = []
        for field in schema:
            if pa.types.is_decimal128(field.type):
                print(f"Converting {field.name} from decimal128 to float64")
                new_fields.append(pa.field(field.name, pa.float64()))
            else:
                new_fields.append(field)

        new_schema = pa.schema(new_fields)

        # Cast Arrow table directly.
        converted_table = parquet_file.cast(new_schema)

        # Write to prepared data bucket.
        print("Writing neighborhoods Parquet to prepared data bucket.")
        parquet_buffer = io.BytesIO()
        pq.write_table(converted_table, parquet_buffer, compression='snappy')
        parquet_buffer.seek(0)

        prepared_blob = storage_client.bucket(prepared_data_bucket).blob(
            "neighborhoods/data.parquet"
        )
        prepared_blob.upload_from_string(
            parquet_buffer.getvalue(), content_type="parquet"
        )

        print(
            f"Prepared neighborhoods Parquet: gs://{prepared_data_bucket}/neighborhoods/data.parquet"
        )

        return (
            "Successfully prepared Philadelphia Neighborhoods Parquet.",
            200,
        )

    except Exception as e:
        print(f"Error: {e}.")
        return (f"Error: {e}.", 500)
