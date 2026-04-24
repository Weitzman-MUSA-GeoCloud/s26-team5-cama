"""
Cloud Function to create the current assessment bins table in BigQuery.

This function creates:
    derived.current_assessment_bins

The table summarizes predicted property assessment values into
equal-width bins of $100,000.

Use: Deploy as a Cloud Function named "create-current-assessment-bins"

"""

import functions_framework
import pathlib
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

DIR_NAME = pathlib.Path(__file__).parent


@functions_framework.http
def create_current_assessment_bins(request):
    """HTTP Cloud Function to create the current assessment bins table.

    Creates or replaces derived.current_assessment_bins with predicted
    property values grouped into $100,000 bins.

    Request:
        request: The HTTP request object.

    Returns:
        A response indicating success or failure.
    """
    try:
        client = bigquery.Client()

        sql = (DIR_NAME / "current_assessment_bins.sql").read_text()

        print("Executing current_assessment_bins.sql...")
        job = client.query(sql)
        job.result()
        print("Created or replaced derived.current_assessment_bins.")

        return ("Successfully created derived.current_assessment_bins.", 200)

    except Exception as e:
        print(f"Error: {e}.")
        return (f"Error: {e}.", 500)
