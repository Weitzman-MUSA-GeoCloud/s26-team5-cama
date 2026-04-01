"""
Cloud Function to create the model training data table in BigQuery.

This function creates or replaces:
    derived.current_assessments_model_training_data

The table contains cleaned OPA property sales data with features
for training a property value prediction model.

Usage:
    Deploy as a Cloud Function named "create-training-data"
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
    job.result()
    return job


@functions_framework.http
def create_training_data(request):
    """HTTP Cloud Function to create the model training data table.

    Creates or replaces derived.current_assessments_model_training_data
    with cleaned OPA property sales filtered for model training.

    Args:
        request: The HTTP request object.

    Returns:
        A response indicating success or failure.
    """
    try:
        project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "musa5090s26-team5")
        client = bigquery.Client()

        context = {
            "project_id": project_id,
        }

        run_sql_file(client, DIR_NAME / "create_training_data.sql", context)
        print("Created or replaced derived.current_assessments_model_training_data.")

        return ("Successfully created model training data table.", 200)

    except Exception as e:
        print(f"Error: {e}.")
        return (f"Error: {e}.", 500)
