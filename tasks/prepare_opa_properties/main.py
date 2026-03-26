"""
Cloud Function to prepare OPA Properties data for BigQuery.

This function reads the raw OPA Properties JSON-L from Cloud Storage,
parses GeoJSON geometry strings, builds a GeoDataFrame, and writes
GeoParquet to the prepared data bucket.

Usage:
    Deploy as a Cloud Function named "prepare-opa-properties"
"""

import functions_framework
import json
import os
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()


def parse_geometry(geom_str):
    """Parse a GeoJSON geometry string to a Shapely geometry object."""
    if geom_str is None or geom_str == "" or pd.isna(geom_str):
        return None
    try:
        geom_dict = json.loads(geom_str)
        return shape(geom_dict)
    except Exception:
        return None


@functions_framework.http
def prepare_opa_properties(request):
    """HTTP Cloud Function to prepare OPA Properties data.

    Reads the raw JSON-L, converts to GeoParquet, and uploads
    to the prepared data bucket.

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
            "opa_properties/data.jsonl"
        )

        # Read JSON-L line by line.
        print("Reading raw OPA Properties JSON-L data.")
        rows = []
        with raw_blob.open("r") as f:
            for line in f:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))

        print(f"Processing {len(rows)} records.")

        df = pd.DataFrame(rows)

        # Parse geometry from GeoJSON strings.
        if "geometry" in df.columns:
            df["geometry"] = df["geometry"].apply(parse_geometry)
        else:
            df["geometry"] = None

        # Create GeoDataFrame in WGS84.
        gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

        # Write GeoParquet to temp file.
        temp_file = "/tmp/opa_properties.parquet"
        print("Writing GeoParquet file.")
        gdf.to_parquet(temp_file, index=False)

        # Upload to prepared data bucket.
        prepared_blob = storage_client.bucket(prepared_data_bucket).blob(
            "opa_properties/data.parquet"
        )
        print("Uploading to Cloud Storage.")
        prepared_blob.upload_from_filename(temp_file)

        print(f"Uploaded to gs://{prepared_data_bucket}/opa_properties/data.parquet")

        return (
            f"Successfully prepared {len(rows)} OPA properties records as GeoParquet.",
            200,
        )

    except Exception as e:
        print(f"Error: {e}.")
        return (f"Error: {e}.", 500)
