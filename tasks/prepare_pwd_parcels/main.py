"""
Cloud Function to prepare PWD Parcels data for BigQuery.

This function reads the raw PWD Parcels JSON-L from Cloud Storage,
parses GeoJSON geometry strings, and writes GeoParquet to the
prepared data bucket.

Usage:
    Deploy as a Cloud Function named "prepare-pwd-parcels"
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
def prepare_pwd_parcels(request):
    """HTTP Cloud Function to prepare PWD Parcels data.

    Reads the raw JSON-L data, parses GeoJSON geometry, and writes
    GeoParquet.

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
        raw_blob = storage_client.bucket(raw_data_bucket).blob("pwd_parcels/data.jsonl")

        # Read JSON-L line by line.
        print("Reading raw PWD Parcels JSON-L data.")
        rows = []
        with raw_blob.open("r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rows.append(json.loads(line))

        print(f"Processing {len(rows)} features.")

        df = pd.DataFrame(rows)

        # Parse geometry from GeoJSON strings.
        if "geometry" in df.columns:
            df["geometry"] = df["geometry"].apply(parse_geometry)
        else:
            df["geometry"] = None

        # Create GeoDataFrame in WGS84.
        gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

        # Fix invalid geometries that BigQuery geography validation rejects.
        print("Validating and fixing geometry.")
        gdf["geometry"] = gdf["geometry"].apply(
            lambda g: g.buffer(0) if g is not None and not g.is_valid else g
        )

        # Write GeoParquet to temp file.
        temp_file = "/tmp/pwd_parcels.parquet"
        print("Writing GeoParquet file.")
        gdf.to_parquet(temp_file, index=False)

        # Upload to prepared data bucket.
        prepared_blob = storage_client.bucket(prepared_data_bucket).blob(
            "pwd_parcels/data.parquet"
        )
        print("Uploading to Cloud Storage.")
        prepared_blob.upload_from_filename(temp_file)

        print(f"Uploaded to gs://{prepared_data_bucket}/pwd_parcels/data.parquet")

        return (
            f"Successfully prepared {len(rows)} PWD parcel records as GeoParquet.",
            200,
        )

    except Exception as e:
        print(f"Error: {e}.")
        return (f"Error: {e}.", 500)
