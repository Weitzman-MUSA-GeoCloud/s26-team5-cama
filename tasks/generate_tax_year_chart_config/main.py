"""
HTTP Cloud Function to generate tax year chart config from BigQuery to Cloud Storage.

This function queries derived.tax_year_assessment_bins for the most recent tax year
and exports the results as a JSON file to the public Cloud Storage bucket.

Usage:
    Deploy as a Cloud Function named "generate-tax-year-chart-config"
"""
import functions_framework
import json
from google.cloud import bigquery
from google.cloud import storage
import os

SQL_QUERY = """
    SELECT
        lower_bound,
        upper_bound,
        property_count,
        tax_year
    FROM `derived.tax_year_assessment_bins`
    WHERE tax_year = (SELECT MAX(tax_year) FROM `derived.tax_year_assessment_bins`)
    ORDER BY lower_bound
"""


@functions_framework.http
def generate_tax_year_chart_config(request):
    try:
        public_bucket = os.getenv("PUBLIC_BUCKET", "musa5090s26-team5-public")

        # Initialize BigQuery client and run query.
        bq_client = bigquery.Client()
        job = bq_client.query(SQL_QUERY)
        results = job.result()
        print("Query executed successfully.")

        # Process results into a list of dictionaries.
        chart_config = [
            {
                "lower_bound": row.lower_bound,
                "upper_bound": row.upper_bound,
                "property_count": row.property_count,
                "tax_year": row.tax_year,
            }
            for row in results
        ]
        print(f"Built JSON chart config with {len(chart_config)} entries.")

        # Upload to public Cloud Storage bucket.
        storage_client = storage.Client()
        bucket = storage_client.bucket(public_bucket)
        blob = bucket.blob("configs/tax_year_assessment_bins.json")
        blob.upload_from_string(
            json.dumps(chart_config),
            content_type="application/json",
        )
        print(f"Uploaded to gs://{public_bucket}/configs/tax_year_assessment_bins.json")

        return ("Tax year chart config generated and uploaded successfully.", 200)

    except Exception as e:
        print(f"Error generating tax year chart config: {e}")
        return (f"Error generating tax year chart config: {e}", 500)
