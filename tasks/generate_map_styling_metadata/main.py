"""
HTTP Cloud Function to export map styling metadata to Cloud Storage.

This function is meant to query derived.current_assessments_model_training_data
and export the relevant map styling metadata to the public GCS bucket.

Usage:
    Deploy as a Cloud Function named "generate-map-styling-metadata"
"""

import functions_framework
import json
from google.cloud import bigquery
from google.cloud import storage
import os

# year_built is commented out for now, in case we want to add it later.
SQL_QUERY = """
    WITH predicted AS (
        SELECT
            MIN(predicted_value) AS predicted_min,
            MAX(predicted_value) AS predicted_max,
            APPROX_QUANTILES(predicted_value, 4) AS predicted_breakpoints)
        FROM `derived.current_assessments`
        WHERE predicted_value IS NOT NULL AND predicted_value >0
    ),
    tax_year AS (
        SELECT
            MIN(market_value) AS tax_year_min,
            MAX(market_value) AS tax_year_max,
            APPROX_QUANTILES(market_value, 4) AS tax_year_breakpoints
        FROM `core.opa_assessments`
        WHERE market_value IS NOT NULL AND market_value >0
            AND year = (SELECT MAX(year) FROM `core.opa_assessments`)
    )
    SELECT * FROM predicted, tax_year
"""


@functions_framework.http
def export_map_styling(request):
    try:
        public_bucket = os.getenv("PUBLIC_BUCKET", "musa5090s26-team5-public")

        # Initialize BQ client and execute
        bq_client = bigquery.Client()
        job = bq_client.query(SQL_QUERY)
        results = job.result()
        print("Executed SQL query to get map styling metadata.")

        # Process results into a dictionary.
        styling_metadata = {}
        for row in results:
            styling_metadata = {
                "predicted_value": {
                    "min": row.predicted_min,
                    "max": row.predicted_max,
                    "breakpoints": list(row.predicted_breakpoints)
                },
                "market_value" : {
                    "min": row.tax_year_min,
                    "max": row.tax_year_max,
                    "breakpoints": list(row.tax_year_breakpoints)
                }
            }
                
            print(f"Processed row: { styling_metadata}")

        # Upload to public GCS bucket.
        storage_client = storage.Client()
        bucket = storage_client.bucket(public_bucket)
        blob = bucket.blob("configs/map_styling_metadata.json")
        blob.upload_from_string(
            json.dumps(styling_metadata),
            content_type="application/json"
        )

        print(f"Uploaded to gs://{public_bucket}/configs/map_styling_metadata.json")

        return ("Successfully exported map styling metadata.", 200)

    except Exception as e:
        print(f"Error exporting map styling metadata: {e}.")
        return (f"Error: {e}.", 500)
