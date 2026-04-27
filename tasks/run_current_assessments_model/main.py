"""
Cloud Function to train and run the current property assessment value model.

On each invocation:
1. Pulls training data from `derived.current_assessments_model_training_data`.
2. Trains three GradientBoostingRegressor models:
   - Main model (least-squares loss) for `predicted_value`
   - Lower quantile model (alpha=0.1) for `predicted_value_lower`
   - Upper quantile model (alpha=0.9) for `predicted_value_upper`
3. Predicts on all residential properties in `core.opa_properties`.
4. Writes results to `derived.current_assessments` (WRITE_TRUNCATE).
5. Uploads feature importances JSON to GCS for the reviewer UI.

Output table schema:
    property_id             STRING
    predicted_value         FLOAT64
    predicted_value_lower   FLOAT64
    predicted_value_upper   FLOAT64
    predicted_at            TIMESTAMP

Refs: Issue #11 (model training), Issue #12 (this deployment).
"""

import json
import os
from datetime import datetime, timezone

import functions_framework
import numpy as np
import pandas as pd
from google.cloud import bigquery, storage
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.preprocessing import LabelEncoder

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "musa5090s26-team5")
PUBLIC_BUCKET = "musa5090s26-team5-public"
FEATURE_IMPORTANCES_BLOB = "configs/model_feature_importances.json"

# Features used for training and prediction
FEATURE_COLS = [
    "total_livable_area",
    "total_area",
    "number_of_bedrooms",
    "number_of_bathrooms",
    "number_stories",
    "garage_spaces",
    "fireplaces",
    "exterior_condition",
    "interior_condition",
    "quality_grade",
    "year_built",
    "zip_code",
    "zoning",
    "building_code_new",
]

# Human-readable labels for the reviewer UI explainability panel
FEATURE_LABELS = {
    "total_livable_area": "Living area (sqft)",
    "total_area": "Total lot area (sqft)",
    "number_of_bedrooms": "Bedrooms",
    "number_of_bathrooms": "Bathrooms",
    "number_stories": "Number of stories",
    "garage_spaces": "Garage spaces",
    "fireplaces": "Fireplaces",
    "exterior_condition": "Exterior condition",
    "interior_condition": "Interior condition",
    "quality_grade": "Quality grade",
    "year_built": "Year built",
    "zip_code": "ZIP code",
    "zoning": "Zoning",
    "building_code_new": "Building code",
}


def load_training_data(client):
    """Pull training data from derived.current_assessments_model_training_data."""
    print("Loading training data from BigQuery...")
    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.derived.current_assessments_model_training_data`
    """
    df = client.query(query).to_dataframe(create_bqstorage_client=False)
    print(f"Loaded {len(df):,} training rows.")
    return df


def load_prediction_data(client):
    """Pull all residential properties from core.opa_properties for prediction."""
    print("Loading residential properties for prediction...")
    cols = ", ".join(["parcel_number"] + FEATURE_COLS)
    query = f"""
        SELECT {cols}
        FROM `{PROJECT_ID}.core.opa_properties`
        WHERE category_code = '1'
            AND total_livable_area IS NOT NULL
            AND total_livable_area > 0
    """
    df = client.query(query).to_dataframe(create_bqstorage_client=False)
    print(f"Loaded {len(df):,} properties for prediction.")
    return df


def prepare_frame(df, label_encoders=None, fit=False):
    """
    Encode categorical columns and impute missing values.
    If fit=True, fit new LabelEncoders and return them.
    If fit=False, apply existing label_encoders.
    Returns (processed_df, label_encoders).
    """
    df = df.copy()
    if label_encoders is None:
        label_encoders = {}

    for col in FEATURE_COLS:
        if col not in df.columns:
            df[col] = np.nan

        if df[col].dtype == object:
            df[col] = df[col].fillna("Unknown")
            if fit:
                le = LabelEncoder()
                df[col] = le.fit_transform(df[col].astype(str))
                label_encoders[col] = le
            else:
                le = label_encoders.get(col)
                if le is not None:
                    known = set(le.classes_)
                    df[col] = df[col].astype(str).apply(
                        lambda x: x if x in known else le.classes_[0]
                    )
                    df[col] = le.transform(df[col])
                else:
                    df[col] = 0
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].fillna(df[col].median())

    return df, label_encoders


def train_models(df_train):
    """
    Train three GBR models:
    - main: least squares (mean prediction)
    - lower: quantile alpha=0.1
    - upper: quantile alpha=0.9
    Returns (model_main, model_lower, model_upper, label_encoders).
    """
    target_col = "sale_price"

    # Filter extreme outliers (top 1%)
    q99 = df_train[target_col].quantile(0.99)
    df_train = df_train[df_train[target_col] <= q99].copy()
    print(f"Training on {len(df_train):,} rows after outlier filtering.")

    X, label_encoders = prepare_frame(df_train[FEATURE_COLS], fit=True)
    y = pd.to_numeric(df_train[target_col], errors="coerce")
    X = X[y.notna()]
    y = y.dropna()

    print("Training main model...")
    model_main = GradientBoostingRegressor(
        n_estimators=200, max_depth=5, learning_rate=0.1, random_state=42
    )
    model_main.fit(X, y)

    print("Training lower quantile model (alpha=0.1)...")
    model_lower = GradientBoostingRegressor(
        loss="quantile", alpha=0.1,
        n_estimators=200, max_depth=5, learning_rate=0.1, random_state=42
    )
    model_lower.fit(X, y)

    print("Training upper quantile model (alpha=0.9)...")
    model_upper = GradientBoostingRegressor(
        loss="quantile", alpha=0.9,
        n_estimators=200, max_depth=5, learning_rate=0.1, random_state=42
    )
    model_upper.fit(X, y)

    return model_main, model_lower, model_upper, label_encoders


def predict_and_write(client, df_pred, model_main, model_lower, model_upper, label_encoders):
    """Run predictions and write results to derived.current_assessments."""
    property_ids = df_pred["parcel_number"].astype(str)

    X_pred, _ = prepare_frame(df_pred[FEATURE_COLS], label_encoders=label_encoders, fit=False)

    print(f"Predicting on {len(X_pred):,} properties...")
    pred_main = model_main.predict(X_pred)
    pred_lower = model_lower.predict(X_pred)
    pred_upper = model_upper.predict(X_pred)

    # Clamp so lower <= predicted <= upper
    pred_lower = np.minimum(pred_lower, pred_main)
    pred_upper = np.maximum(pred_upper, pred_main)

    predicted_at = datetime.now(timezone.utc)

    out = pd.DataFrame({
        "property_id": property_ids.values,
        "predicted_value": pred_main.astype(float),
        "predicted_value_lower": pred_lower.astype(float),
        "predicted_value_upper": pred_upper.astype(float),
        "predicted_at": predicted_at,
    })

    # Drop non-finite predictions
    finite_mask = (
        np.isfinite(out["predicted_value"]) &
        np.isfinite(out["predicted_value_lower"]) &
        np.isfinite(out["predicted_value_upper"])
    )
    out = out[finite_mask]

    table = f"{PROJECT_ID}.derived.current_assessments"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("property_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("predicted_value", "FLOAT64", mode="REQUIRED"),
            bigquery.SchemaField("predicted_value_lower", "FLOAT64", mode="REQUIRED"),
            bigquery.SchemaField("predicted_value_upper", "FLOAT64", mode="REQUIRED"),
            bigquery.SchemaField("predicted_at", "TIMESTAMP", mode="REQUIRED"),
        ],
    )
    print(f"Writing {len(out):,} predictions to {table}.")
    job = client.load_table_from_dataframe(out, table, job_config=job_config)
    job.result()
    return len(out)


def upload_feature_importances(model_main, trained_at):
    """Upload feature importances JSON to GCS for the reviewer UI."""
    importances = model_main.feature_importances_
    features = [
        {
            "feature": col,
            "label": FEATURE_LABELS.get(col, col),
            "importance": round(float(imp), 6),
        }
        for col, imp in zip(FEATURE_COLS, importances)
    ]
    features.sort(key=lambda x: x["importance"], reverse=True)

    payload = {
        "model": "GradientBoostingRegressor",
        "trained_at": trained_at.isoformat(),
        "features": features,
    }

    storage_client = storage.Client()
    bucket = storage_client.bucket(PUBLIC_BUCKET)
    blob = bucket.blob(FEATURE_IMPORTANCES_BLOB)
    blob.upload_from_string(
        json.dumps(payload, indent=2),
        content_type="application/json",
    )
    print(f"Uploaded feature importances to gs://{PUBLIC_BUCKET}/{FEATURE_IMPORTANCES_BLOB}.")


@functions_framework.http
def run_current_assessments_model(request):
    """HTTP Cloud Function entry point.

    Returns 200 with a summary string on success, 500 on error.
    """
    try:
        client = bigquery.Client()
        trained_at = datetime.now(timezone.utc)

        # 1. Load training data and train models
        df_train = load_training_data(client)
        model_main, model_lower, model_upper, label_encoders = train_models(df_train)

        # 2. Load all residential properties and predict
        df_pred = load_prediction_data(client)
        n_written = predict_and_write(
            client, df_pred, model_main, model_lower, model_upper, label_encoders
        )

        # 3. Upload feature importances for reviewer UI
        upload_feature_importances(model_main, trained_at)

        msg = f"Wrote {n_written:,} predictions to derived.current_assessments."
        print(msg)
        return (msg, 200)

    except Exception as e:
        print(f"Error: {e}.")
        return (f"Error: {e}.", 500)
