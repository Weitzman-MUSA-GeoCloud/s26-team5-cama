"""
Cloud Function to load tax year bins into BigQuery.

This function creates or replaces:
1. An external table in the source dataset backed by the prepared JSON-L file.
2. An internal table in the core dataset with an added property_id field.

Usage:
    Deploy as a Cloud Function named "load-tax-year-bins"
"""

import functions_framework
import pathlib
import os
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

DIR_NAME = pathlib.Path(__file__).parent


@functions_framework.http
def create_tax_year_assessment_bins(request):
    try:
        client = bigquery.Client()

        sql = (DIR_NAME / "tax_year_assessment_bins.sql").read_text()

        print("Executing tax_year_assessment_bins.sql...")
        job = client.query(sql)
        job.result()
        print("Created or replaced derived.tax_year_assessment_bins.")

        return ("Successfully created derived.tax_year_assessment_bins.", 200)

    except Exception as e:
        print(f"Error: {e}.")
        return (f"Error: {e}.", 500)
