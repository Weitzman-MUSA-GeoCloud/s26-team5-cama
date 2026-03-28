"""
Cloud Function to load PWD Parcels data into BigQuery.

This function creates or replaces:
1. An external table in the source dataset backed by the prepared JSON-L file.
2. An internal table in the core dataset with an added property_id field.

Usage:
    Deploy as a Cloud Function named "load-pwd-parcels"
"""

import functions_framework
import pathlib
import os
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

DIR_NAME = pathlib.Path(__file__).parent


def render_template(sql_query_template, context):
    """Render a SQL template by substituting {var} placeholders.

    Args:
        sql_query_template: SQL string with {var} placeholders.
        context: Dictionary of variable names to values.

    Returns:
        The rendered SQL string.
    """
    return sql_query_template.format(**context)


def run_sql_file(client, sql_file_path, context):
    """Execute a SQL file with variable substitution.

    Args:
        client: BigQuery client instance.
        sql_file_path: Path to the SQL file.
        context: Dictionary of variables to substitute in the SQL.

    Returns:
        The query job result.
    """
    with open(sql_file_path, "r", encoding="utf-8") as f:
        sql_query_template = f.read()

    sql_query = render_template(sql_query_template, context)

    print(f"Executing SQL from {sql_file_path}.")
    job = client.query(sql_query)
    # Wait for completion.
    job.result()
    return job


@functions_framework.http
def load_pwd_parcels(request):
    """HTTP Cloud Function to load PWD Parcels into BigQuery.

    Creates or replaces the source.pwd_parcels external table and
    the core.pwd_parcels internal table.

    Args:
        request: The HTTP request object.

    Returns:
        A response indicating success or failure.
    """
    try:
        project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "musa5090s26-team5")
        client = bigquery.Client()

        # Context for SQL template rendering.
        context = {
            "project_id": project_id,
            "bucket_name": os.getenv(
                "DATA_LAKE_BUCKET", "musa5090s26-team5-prepared_data"
            ),
        }

        # Run the source table SQL.
        run_sql_file(client, DIR_NAME / "source_pwd_parcels.sql", context)
        print("Created or replaced source.pwd_parcels external table.")

        # Run the core table SQL.
        run_sql_file(client, DIR_NAME / "core_pwd_parcels.sql", context)
        print("Created or replaced core.pwd_parcels table.")

        return ("Successfully loaded PWD Parcels into BigQuery.", 200)

    except Exception as e:
        print(f"Error: {e}.")
        return (f"Error: {e}.", 500)
