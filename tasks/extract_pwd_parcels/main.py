"""
Cloud Function to extract PWD Parcels data from ArcGIS Hub.

This function downloads the PWD Stormwater Billing Parcels dataset as
GeoJSON, converts each feature to a JSON-L row, and uploads to Cloud Storage.

Usage:
    Deploy as a Cloud Function named "extract-pwd-parcels"
"""

import functions_framework
import requests
import json
import os
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

# GeoJSON download link for PWD Parcels.
PWD_PARCELS_URL = (
    "https://hub.arcgis.com/api/v3/datasets/"
    "84baed491de44f539889f2af178ad85c_0/downloads/data"
    "?format=geojson&spatialRefId=4326&where=1%3D1"
)


@functions_framework.http
def extract_pwd_parcels(request):
    """HTTP Cloud Function to extract PWD Parcels data.

    Downloads the PWD Parcels GeoJSON, converts each feature to a
    JSON-L row with properties and geometry, and uploads to Cloud Storage.

    Args:
        request: The HTTP request object.

    Returns:
        A response indicating success or failure.
    """
    try:
        raw_data_bucket = os.getenv("RAW_DATA_BUCKET", "musa5090s26-team5-raw_data")

        # Download the GeoJSON file.
        print("Downloading PWD Parcels GeoJSON.")
        response = requests.get(PWD_PARCELS_URL, timeout=1800)
        response.raise_for_status()

        data = response.json()
        features = data["features"]
        print(f"Downloaded {len(features)} features.")

        # Convert GeoJSON features to JSON-L and upload.
        storage_client = storage.Client()
        bucket = storage_client.bucket(raw_data_bucket)
        blob = bucket.blob("pwd_parcels/data.jsonl")

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

        print(f"Uploaded to gs://{raw_data_bucket}/pwd_parcels/data.jsonl")

        return (
            f"Successfully extracted {len(features)} PWD parcel records as JSON-L.",
            200,
        )

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}.")
        return (f"Error fetching data: {e}.", 500)
    except Exception as e:
        print(f"Error: {e}.")
        return (f"Error: {e}.", 500)
