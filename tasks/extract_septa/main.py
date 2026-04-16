"""
Cloud Function to extract SEPTA Stations data.

This function downloads the SEPTA Stations dataset as a CSV
from the City of Philadelphia and uploads to Cloud Storage.

Usage:
    Deploy as a Cloud Function named "extract-septa"
"""

import functions_framework
import requests
import os
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

# Direct CSV download link for SEPTA Stations.
SEPTA_STATIONS_URL = "https://hub.arcgis.com/api/v3/datasets/b227f3ddbe3e47b4bcc7b7c65ef2cef6_0/downloads/data?format=csv&spatialRefId=3857&where=1%3D1"


@functions_framework.http
def extract_septa(request):
    """HTTP Cloud Function to extract SEPTA Stations data.

    Downloads the SEPTA Stations CSV file and uploads
    to Cloud Storage.

    Args:
        request: The HTTP request object.

    Returns:
        A response indicating success or failure.
    """
    try:
        raw_data_bucket = os.getenv("RAW_DATA_BUCKET", "musa5090s26-team5-raw_data")

        # Download the stations CSV file.
        print("Downloading SEPTA Stations CSV.")
        response = requests.get(SEPTA_STATIONS_URL, timeout=1800)
        response.raise_for_status()

        # Upload to Cloud Storage.
        storage_client = storage.Client()
        bucket = storage_client.bucket(raw_data_bucket)
        blob = bucket.blob("septa/data.csv")

        print("Uploading CSV to Cloud Storage.")
        blob.upload_from_string(response.content, content_type="text/csv")

        print(f"Uploaded to gs://{raw_data_bucket}/septa/data.csv")

        return (
            "Successfully extracted SEPTA Stations as CSV.",
            200,
        )

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}.")
        return (f"Error fetching data: {e}.", 500)
    except Exception as e:
        print(f"Error: {e}.")
        return (f"Error: {e}.", 500)
