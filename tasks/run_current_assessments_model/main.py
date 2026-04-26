"""
Cloud Function to predict current property assessment values.

Loads the pre-trained model bundle from `model.pkl`, reads features for all
residential properties from BigQuery, applies the same preprocessing the
training notebook applied, runs the model, and writes predictions to
`derived.current_assessments`.

The output table has three columns: `property_id`, `predicted_value`,
`predicted_at`. It is overwritten on each run (WRITE_TRUNCATE).

Usage:
    Deploy as a Cloud Function named "predict-assessments".

Refs: Issue #11 (model training), Issue #12 (this deployment).
"""

import os
import pathlib
from datetime import datetime, timezone

import functions_framework
import joblib
import numpy as np
import pandas as pd
from google.cloud import bigquery

DIR_NAME = pathlib.Path(__file__).parent
MODEL_PATH = DIR_NAME / "model.pkl"
SQL_PATH = DIR_NAME / "predict_assessments.sql"

# Feature column order MUST match how the model was trained.
# This is the value of `bundle["model"].feature_names_in_` from the saved bundle.
FEATURE_COLS = [
    "total_livable_area",
    "number_of_bathrooms",
    "property_age",
    "interior_condition",
    "assessed_value_2023",
    "assessed_value_2024",
    "assessed_value_2025",
    "neighborhood",
    "sale_year",
    "neighborhood_median_price",
]


def render_template(sql_query_template, context):
    """Render a SQL template by substituting {var} placeholders."""
    return sql_query_template.format(**context)


def load_features(client, project_id):
    """Read raw features for all residential properties from BigQuery."""
    with open(SQL_PATH, "r", encoding="utf-8") as f:
        sql = render_template(f.read(), {"project_id": project_id})
    print(f"Reading features from {SQL_PATH.name}.")
    df = client.query(sql).to_dataframe()
    print(f"Loaded {len(df):,} property rows.")
    return df


def preprocess(df, bundle):
    """Apply the same preprocessing the training notebook applied.

    1. Derive `neighborhood_median_price` from `bundle["neighborhood_price_map"]`,
       falling back to `bundle["global_price_fallback"]` for unseen neighborhoods.
    2. Impute missing values from `bundle["medians"]`.
    3. Label-encode `neighborhood` using `bundle["label_encoders"]["neighborhood"]`.
       Unseen neighborhoods are encoded as -1.
    """
    medians = bundle["medians"]
    price_map = bundle["neighborhood_price_map"]
    fallback = bundle["global_price_fallback"]
    label_encoders = bundle["label_encoders"]

    # 1. Derive neighborhood_median_price from the bundled price map.
    df["neighborhood_median_price"] = (
        df["neighborhood"].map(price_map).fillna(fallback)
    )

    # 2. Impute numeric features from training-time medians.
    for col, value in medians.items():
        if col in df.columns:
            df[col] = df[col].fillna(value)

    # 3. Label-encode neighborhood. Unseen neighborhoods get -1.
    le = label_encoders["neighborhood"]
    known = set(le.classes_)
    df["neighborhood"] = df["neighborhood"].fillna("__UNKNOWN__")
    encoded = pd.Series(-1, index=df.index, dtype=np.int64)
    mask_known = df["neighborhood"].isin(known)
    if mask_known.any():
        encoded.loc[mask_known] = le.transform(df.loc[mask_known, "neighborhood"])
    df["neighborhood"] = encoded

    return df


def predict_and_write(client, project_id, df, model):
    """Run model.predict and overwrite `derived.current_assessments`."""
    print(f"Running predictions on {len(df):,} properties.")
    feature_matrix = df[FEATURE_COLS]
    predictions = model.predict(feature_matrix)
    predicted_at = datetime.now(timezone.utc)

    out = pd.DataFrame(
        {
            "property_id": df["property_id"].astype(str),
            "predicted_value": predictions.astype(float),
            "predicted_at": predicted_at,
        }
    )
    # Drop any non-finite predictions (NaN / inf) defensively.
    out = out[np.isfinite(out["predicted_value"])]

    table = f"{project_id}.derived.current_assessments"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("property_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("predicted_value", "FLOAT64", mode="REQUIRED"),
            bigquery.SchemaField("predicted_at", "TIMESTAMP", mode="REQUIRED"),
        ],
    )
    print(f"Writing {len(out):,} predictions to {table}.")
    job = client.load_table_from_dataframe(out, table, job_config=job_config)
    job.result()
    return len(out)


@functions_framework.http
def predict_assessments(request):
    """HTTP Cloud Function entry point.

    Returns a 200 with a summary string on success, 500 on error.
    """
    try:
        project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "musa5090s26-team5")
        bundle = joblib.load(MODEL_PATH)
        client = bigquery.Client()

        df = load_features(client, project_id)
        df = preprocess(df, bundle)
        n_written = predict_and_write(client, project_id, df, bundle["model"])

        msg = f"Wrote {n_written:,} predictions to derived.current_assessments."
        print(msg)
        return (msg, 200)

    except Exception as e:
        print(f"Error: {e}.")
        return (f"Error: {e}.", 500)
