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

# Features used for training and prediction — must match columns produced by
# tasks/create_training_data/create_training_data.sql
FEATURE_COLS = [
    "total_livable_area",
    "total_area",
    "year_built",
    "property_age",
    "exterior_condition",
    "interior_condition",
    "condition_score",
    "number_of_bedrooms",
    "number_of_bathrooms",
    "zoning",
    "assessed_value_2023",
    "assessed_value_2024",
    "assessed_value_2025",
    "pct_change_2023_to_2025",
    "lot_area_sqft",
    "lot_perimeter",
    "lot_shape_ratio",
    "dist_to_septa_miles",
    "neighborhood",
]

# Human-readable labels for the reviewer UI explainability panel
FEATURE_LABELS = {
    "total_livable_area": "Living area (sqft)",
    "total_area": "Total lot area (sqft)",
    "year_built": "Year built",
    "property_age": "Property age (years)",
    "exterior_condition": "Exterior condition",
    "interior_condition": "Interior condition",
    "condition_score": "Avg condition score",
    "number_of_bedrooms": "Bedrooms",
    "number_of_bathrooms": "Bathrooms",
    "zoning": "Zoning",
    "assessed_value_2023": "Assessed value 2023",
    "assessed_value_2024": "Assessed value 2024",
    "assessed_value_2025": "Assessed value 2025",
    "pct_change_2023_to_2025": "% change 2023→2025",
    "lot_area_sqft": "Lot area (sqft)",
    "lot_perimeter": "Lot perimeter (ft)",
    "lot_shape_ratio": "Lot shape ratio",
    "dist_to_septa_miles": "Distance to SEPTA (mi)",
    "neighborhood": "Neighborhood",
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
    """Pull all residential properties with the same features used for training."""
    print("Loading residential properties for prediction...")
    query = f"""
        WITH assessments_pivot AS (
            SELECT
                parcel_number,
                MAX(CASE WHEN year = 2023.0 THEN market_value END) AS assessed_value_2023,
                MAX(CASE WHEN year = 2024.0 THEN market_value END) AS assessed_value_2024,
                MAX(CASE WHEN year = 2025.0 THEN market_value END) AS assessed_value_2025
            FROM `{PROJECT_ID}.core.opa_assessments`
            WHERE market_value IS NOT NULL AND market_value > 0
            GROUP BY parcel_number
        ),

        pwd AS (
            SELECT
                brt_id,
                shape__area AS lot_area_sqft,
                shape__length AS lot_perimeter,
                SAFE_DIVIDE(shape__area, shape__length * shape__length) AS lot_shape_ratio
            FROM `{PROJECT_ID}.core.pwd_parcels`
            WHERE brt_id IS NOT NULL AND shape__area > 0
        ),

        septa_dist AS (
            SELECT
                p.parcel_number,
                MIN(ST_DISTANCE(
                    p.geometry,
                    ST_GEOGPOINT(
                        SAFE_CAST(s.longitude AS FLOAT64),
                        SAFE_CAST(s.latitude AS FLOAT64)
                    )
                )) / 1609.34 AS dist_to_septa_miles
            FROM `{PROJECT_ID}.core.opa_properties` AS p
            CROSS JOIN `{PROJECT_ID}.core.septa` AS s
            WHERE
                p.category_code = '1'
                AND s.longitude IS NOT NULL
                AND s.latitude IS NOT NULL
                AND p.geometry IS NOT NULL
            GROUP BY p.parcel_number
        ),

        prop_neighborhood AS (
            SELECT
                p.parcel_number,
                n.name AS neighborhood
            FROM `{PROJECT_ID}.core.opa_properties` AS p
            INNER JOIN `{PROJECT_ID}.core.neighborhoods` AS n
                ON ST_CONTAINS(ST_GEOGFROMWKB(n.geometry), p.geometry)
            WHERE
                p.category_code = '1'
                AND p.geometry IS NOT NULL
        )

        SELECT
            p.parcel_number,
            SAFE_CAST(p.total_livable_area AS FLOAT64) AS total_livable_area,
            SAFE_CAST(p.total_area AS FLOAT64) AS total_area,
            SAFE_CAST(p.year_built AS FLOAT64) AS year_built,
            EXTRACT(YEAR FROM CURRENT_DATE())
                - SAFE_CAST(p.year_built AS INT64) AS property_age,
            SAFE_CAST(p.exterior_condition AS FLOAT64) AS exterior_condition,
            SAFE_CAST(p.interior_condition AS FLOAT64) AS interior_condition,
            (
                SAFE_CAST(p.exterior_condition AS FLOAT64)
                + SAFE_CAST(p.interior_condition AS FLOAT64)
            ) / 2.0 AS condition_score,
            SAFE_CAST(p.number_of_bedrooms AS FLOAT64) AS number_of_bedrooms,
            SAFE_CAST(p.number_of_bathrooms AS FLOAT64) AS number_of_bathrooms,
            p.zoning,
            a.assessed_value_2023,
            a.assessed_value_2024,
            a.assessed_value_2025,
            ROUND(
                SAFE_DIVIDE(
                    a.assessed_value_2025 - a.assessed_value_2023,
                    a.assessed_value_2023
                ) * 100,
                2
            ) AS pct_change_2023_to_2025,
            pwd.lot_area_sqft,
            pwd.lot_perimeter,
            pwd.lot_shape_ratio,
            sd.dist_to_septa_miles,
            pn.neighborhood
        FROM `{PROJECT_ID}.core.opa_properties` AS p
        LEFT JOIN assessments_pivot AS a
            ON CAST(a.parcel_number AS STRING) = p.parcel_number
        LEFT JOIN pwd
            ON CAST(pwd.brt_id AS STRING) = p.parcel_number
        LEFT JOIN septa_dist AS sd
            ON sd.parcel_number = p.parcel_number
        LEFT JOIN prop_neighborhood AS pn
            ON pn.parcel_number = p.parcel_number
        WHERE
            p.category_code = '1'
            AND p.total_livable_area IS NOT NULL
            AND SAFE_CAST(p.total_livable_area AS FLOAT64) > 0
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
