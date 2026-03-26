"""
Cloud Function to extract OPA Properties data from the Philadelphia CARTO API.

This function downloads the OPA Properties dataset as GeoJSON, converts
each feature to a JSON-L row, and uploads to Cloud Storage.

Usage:
    Deploy as a Cloud Function named "extract-opa-properties"
"""

import functions_framework
import requests
import json
import os
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

# GeoJSON download link for OPA Properties.
OPA_PROPERTIES_URL = (
    "https://phl.carto.com/api/v2/sql"
    "?filename=opa_properties_public"
    "&format=geojson"
    "&skipfields=cartodb_id"
    "&q=SELECT+*+FROM+opa_properties_public"
)


@functions_framework.http
def extract_opa_properties(request):
    """HTTP Cloud Function to extract OPA Properties data.

    Downloads the OPA Properties GeoJSON, converts each feature to a
    JSON-L row with properties and geometry, and uploads to Cloud Storage.

    Args:
        request: The HTTP request object.

    Returns:
        A response indicating success or failure.
    """
    try:
        raw_data_bucket = os.getenv("RAW_DATA_BUCKET", "musa5090s26-team5-raw_data")

        # Download the GeoJSON file.
        print("Downloading OPA Properties GeoJSON.")
        response = requests.get(OPA_PROPERTIES_URL, timeout=1800)
        response.raise_for_status()

        data = response.json()
        features = data["features"]
        print(f"Downloaded {len(features)} features.")

        # Convert GeoJSON features to JSON-L and upload.
        storage_client = storage.Client()
        bucket = storage_client.bucket(raw_data_bucket)
        blob = bucket.blob("opa_properties/data.jsonl")

        print("Converting to JSON-L and uploading to Cloud Storage.")
        with blob.open("w") as f:
            for feature in features:
                row = feature["properties"]
                row["geometry"] = (
                    json.dumps(feature["geometry"])
                    if feature["geometry"] and feature["geometry"].get("coordinates")
                    else None
                )
                f.write(json.dumps(row) + "\n")

        print(f"Uploaded to gs://{raw_data_bucket}/opa_properties/data.jsonl")

        return (
            f"Successfully extracted {len(features)} OPA properties records as JSON-L.",
            200,
        )

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}.")
        return (f"Error fetching data: {e}.", 500)
    except Exception as e:
        print(f"Error: {e}.")
        return (f"Error: {e}.", 500)
