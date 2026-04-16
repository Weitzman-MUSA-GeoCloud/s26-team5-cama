"""
Cloud Function to extract Philadelphia Neighborhoods data.

This function downloads the Philadelphia Neighborhoods dataset as a parquet
from the City of Philadelphia and uploads to Cloud Storage.

Usage:
    Deploy as a Cloud Function named "extract-neighborhoods"
"""

import functions_framework
import requests
import os
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

# Direct parquet download link for Philadelphia Neighborhoods.
NEIGHBORHOODS_URL = "https://raw.githubusercontent.com/opendataphilly/odp-data-storage/master/philadelphia-neighborhoods/philadelphia-neighborhoods.parquet"


@functions_framework.http
def extract_neighborhoods(request):
    """HTTP Cloud Function to extract Philadelphia Neighborhoods data.

    Downloads the Philadelphia Neighborhoods parquet file and uploads
    to Cloud Storage.

    Args:
        request: The HTTP request object.

    Returns:
        A response indicating success or failure.
    """
    try:
        raw_data_bucket = os.getenv("RAW_DATA_BUCKET", "musa5090s26-team5-raw_data")

        # Download the neighborhoods parquet file.
        print("Downloading Philadelphia Neighborhoods parquet.")
        response = requests.get(NEIGHBORHOODS_URL, timeout=1800)
        response.raise_for_status()

        # Upload to Cloud Storage.
        storage_client = storage.Client()
        bucket = storage_client.bucket(raw_data_bucket)
        blob = bucket.blob("neighborhoods/data.parquet")

        print("Uploading parquet to Cloud Storage.")
        blob.upload_from_string(response.content, content_type="parquet")

        print(f"Uploaded to gs://{raw_data_bucket}/neighborhoods/data.parquet")

        return (
            "Successfully extracted Philadelphia Neighborhoods as parquet.",
            200,
        )

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}.")
        return (f"Error fetching data: {e}.", 500)
    except Exception as e:
        print(f"Error: {e}.")
        return (f"Error: {e}.", 500)
