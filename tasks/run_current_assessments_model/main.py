"""
Cloud Function to train the current assessment value model and load
predictions into BigQuery.

Issue #12: train weekly on `derived.current_assessments_model_training_data`
and write predictions for every residential property to
`derived.current_assessments`.

This function adds two features on top of the basic point
prediction described in the issue:

1. Margin of Error (MOE)
   - Two extra Gradient Boosting models are trained with `loss="quantile"`
     at the 10th and 90th percentiles. Together with the median model
     they produce an 80% prediction interval (`predicted_value_lower`
     and `predicted_value_upper`).

2. Explainability
   - Global feature importances from the trained median model are
     written to a JSON file in the public bucket so the front-end can
     show "what drove this prediction" for any property.

The output table `derived.current_assessments` has the columns:
    property_id, predicted_value, predicted_value_lower,
    predicted_value_upper, predicted_at

The output config file is:
    gs://<public bucket>/configs/model_feature_importances.json

Usage:
    Deploy as a Cloud Function named "run-current-assessments-model".
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
PUBLIC_BUCKET = os.getenv("PUBLIC_BUCKET", "musa5090s26-team5-public")
OUTPUT_TABLE = f"{PROJECT_ID}.derived.current_assessments"
FEATURE_IMPORTANCES_BLOB = "configs/model_feature_importances.json"

# Features mirror EDA notebook (eda/train_model.ipynb). Keeping
# same list keeps function consistent with offline exploration
# work and avoids surprises when comparing results.
SOURCE_FEATURES = [
    "total_livable_area",
    "number_of_bathrooms",
    "property_age",
    "interior_condition",
    "assessed_value_2023",
    "assessed_value_2024",
    "assessed_value_2025",
    "neighborhood",
    "sale_year",
]
CATEGORICAL_FEATURES = ["neighborhood"]

# Human-readable labels for front-end explainability panel.
FEATURE_LABELS = {
    "total_livable_area": "Living area (sqft)",
    "number_of_bathrooms": "Bathrooms",
    "property_age": "Property age (years)",
    "interior_condition": "Interior condition",
    "assessed_value_2023": "2023 assessed value",
    "assessed_value_2024": "2024 assessed value",
    "assessed_value_2025": "2025 assessed value",
    "neighborhood": "Neighborhood",
    "neighborhood_median_price": "Neighborhood typical sale price",
    "sale_year": "Reference year",
}


def load_training_data(client):
    """Load the prepared training table built by `create_training_data`."""
    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.derived.current_assessments_model_training_data`
        WHERE total_livable_area IS NOT NULL
          AND total_livable_area > 0
    """
    return client.query(query).to_dataframe(create_bqstorage_client=False)


def load_prediction_features(client):
    """Pull the same feature columns for every residential property.

    This SQL is deliberately the same shape as the training query so
    feature engineering applied at training time also works here.
    """
    query = f"""
        WITH medians AS (
            SELECT
                PERCENTILE_CONT(SAFE_CAST(total_livable_area AS FLOAT64), 0.5) OVER ()
                    AS median_total_livable_area,
                PERCENTILE_CONT(SAFE_CAST(year_built AS FLOAT64), 0.5) OVER ()
                    AS median_year_built,
                PERCENTILE_CONT(SAFE_CAST(number_of_bathrooms AS FLOAT64), 0.5) OVER ()
                    AS median_bathrooms
            FROM `{PROJECT_ID}.core.opa_properties`
            WHERE category_code = '1'
            LIMIT 1
        ),
        mode_conditions AS (
            SELECT
                APPROX_TOP_COUNT(
                    SAFE_CAST(interior_condition AS FLOAT64), 1
                )[OFFSET(0)].value AS mode_interior
            FROM `{PROJECT_ID}.core.opa_properties`
            WHERE category_code = '1'
              AND interior_condition IS NOT NULL
        ),
        assessments_pivot AS (
            SELECT
                parcel_number,
                MAX(CASE WHEN year = 2023.0 THEN market_value END) AS assessed_value_2023,
                MAX(CASE WHEN year = 2024.0 THEN market_value END) AS assessed_value_2024,
                MAX(CASE WHEN year = 2025.0 THEN market_value END) AS assessed_value_2025
            FROM `{PROJECT_ID}.core.opa_assessments`
            WHERE market_value IS NOT NULL AND market_value > 0
            GROUP BY parcel_number
        ),
        prop_neighborhood AS (
            SELECT
                p.parcel_number,
                n.name AS neighborhood
            FROM `{PROJECT_ID}.core.opa_properties` AS p
            INNER JOIN `{PROJECT_ID}.core.neighborhoods` AS n
                ON ST_CONTAINS(ST_GEOGFROMWKB(n.geometry), p.geometry)
            WHERE p.category_code = '1'
              AND p.geometry IS NOT NULL
        )
        SELECT
            p.parcel_number,
            COALESCE(SAFE_CAST(p.total_livable_area AS FLOAT64), m.median_total_livable_area)
                AS total_livable_area,
            COALESCE(SAFE_CAST(p.number_of_bathrooms AS FLOAT64), m.median_bathrooms)
                AS number_of_bathrooms,
            EXTRACT(YEAR FROM CURRENT_DATE())
                - CAST(COALESCE(SAFE_CAST(p.year_built AS FLOAT64), m.median_year_built) AS INT64)
                AS property_age,
            COALESCE(SAFE_CAST(p.interior_condition AS FLOAT64), mc.mode_interior)
                AS interior_condition,
            a.assessed_value_2023,
            a.assessed_value_2024,
            a.assessed_value_2025,
            pn.neighborhood,
            EXTRACT(YEAR FROM CURRENT_DATE()) AS sale_year
        FROM `{PROJECT_ID}.core.opa_properties` AS p
        CROSS JOIN medians AS m
        CROSS JOIN mode_conditions AS mc
        LEFT JOIN assessments_pivot AS a
            ON CAST(a.parcel_number AS STRING) = p.parcel_number
        LEFT JOIN prop_neighborhood AS pn
            ON pn.parcel_number = p.parcel_number
        WHERE p.category_code = '1'
          AND SAFE_CAST(p.total_livable_area AS FLOAT64) > 0
    """
    return client.query(query).to_dataframe(create_bqstorage_client=False)


def prepare_training_frame(df):
    """Return (X, y, encoders, medians, neighborhood_price_map, fallback_price)."""
    df = df[SOURCE_FEATURES + ["sale_price"]].copy()

    # Spatial lag feature of median sale price per neighborhood.
    neighborhood_price_map = (
        df.groupby("neighborhood")["sale_price"].median().to_dict()
    )
    fallback_price = float(df["sale_price"].median())
    df["neighborhood_median_price"] = (
        df["neighborhood"].map(neighborhood_price_map).fillna(fallback_price)
    )

    features = SOURCE_FEATURES + ["neighborhood_median_price"]

    label_encoders = {}
    medians = {}
    for col in features:
        if col in CATEGORICAL_FEATURES or df[col].dtype == "object":
            df[col] = df[col].fillna("Unknown").astype(str)
            le = LabelEncoder()
            df[col] = le.fit_transform(df[col])
            label_encoders[col] = le
        else:
            med = float(df[col].median())
            df[col] = df[col].fillna(med)
            medians[col] = med

    # Trim top 1% of sale prices to reduce outlier pull.
    cap = df["sale_price"].quantile(0.99)
    df = df[df["sale_price"] <= cap]

    X = df[features]
    y = df["sale_price"]
    return X, y, features, label_encoders, medians, neighborhood_price_map, fallback_price


def encode_prediction_frame(
    df, features, label_encoders, medians, neighborhood_price_map, fallback_price
):
    """Apply training-time encoders/medians to the full property frame."""
    df = df.copy()
    df["neighborhood_median_price"] = (
        df["neighborhood"].fillna("Unknown").map(neighborhood_price_map).fillna(fallback_price)
    )
    for col in features:
        if col in label_encoders:
            le = label_encoders[col]
            known = set(le.classes_)
            fallback_label = "Unknown" if "Unknown" in known else le.classes_[0]
            df[col] = (
                df[col]
                .fillna("Unknown")
                .astype(str)
                .apply(lambda v: v if v in known else fallback_label)
            )
            df[col] = le.transform(df[col])
        else:
            med = medians.get(col, float(df[col].median()))
            df[col] = df[col].fillna(med)
    return df[features]


def train_models(X, y):
    """Train a median regressor plus 10th/90th percentile quantile models.

    The two quantile models give an 80% prediction interval used as the
    margin of error in the UI.
    """
    common = dict(n_estimators=200, max_depth=5, learning_rate=0.1, random_state=42)

    median_model = GradientBoostingRegressor(**common)
    median_model.fit(X, y)

    lower_model = GradientBoostingRegressor(loss="quantile", alpha=0.1, **common)
    lower_model.fit(X, y)

    upper_model = GradientBoostingRegressor(loss="quantile", alpha=0.9, **common)
    upper_model.fit(X, y)

    return median_model, lower_model, upper_model


def upload_feature_importances(median_model, features):
    """Write a small JSON of feature importances to the public bucket."""
    importances = sorted(
        (
            {
                "feature": feat,
                "label": FEATURE_LABELS.get(feat, feat),
                "importance": float(imp),
            }
            for feat, imp in zip(features, median_model.feature_importances_)
        ),
        key=lambda d: d["importance"],
        reverse=True,
    )
    payload = {
        "model": "GradientBoostingRegressor",
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "features": importances,
    }
    storage_client = storage.Client()
    blob = storage_client.bucket(PUBLIC_BUCKET).blob(FEATURE_IMPORTANCES_BLOB)
    blob.upload_from_string(json.dumps(payload), content_type="application/json")
    print(f"Uploaded gs://{PUBLIC_BUCKET}/{FEATURE_IMPORTANCES_BLOB}")


def write_predictions(client, results):
    """Replace `derived.current_assessments` with the new predictions."""
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("property_id", "STRING"),
            bigquery.SchemaField("predicted_value", "FLOAT64"),
            bigquery.SchemaField("predicted_value_lower", "FLOAT64"),
            bigquery.SchemaField("predicted_value_upper", "FLOAT64"),
            bigquery.SchemaField("predicted_at", "TIMESTAMP"),
        ],
    )
    job = client.load_table_from_dataframe(results, OUTPUT_TABLE, job_config=job_config)
    job.result()
    print(f"Loaded {len(results):,} rows to {OUTPUT_TABLE}")


@functions_framework.http
def run_current_assessments_model(request):
    """HTTP entry point. Trains the model and writes predictions."""
    try:
        client = bigquery.Client(project=PROJECT_ID)

        print("Loading training data...")
        df_train = load_training_data(client)
        print(f"Training rows: {len(df_train):,}")

        (
            X,
            y,
            features,
            label_encoders,
            medians,
            neighborhood_price_map,
            fallback_price,
        ) = prepare_training_frame(df_train)

        print("Training models (median + lower + upper quantile)...")
        median_model, lower_model, upper_model = train_models(X, y)

        print("Loading all residential properties for prediction...")
        df_all = load_prediction_features(client)
        print(f"Properties to score: {len(df_all):,}")

        X_pred = encode_prediction_frame(
            df_all,
            features,
            label_encoders,
            medians,
            neighborhood_price_map,
            fallback_price,
        )

        predicted = median_model.predict(X_pred)
        lower = lower_model.predict(X_pred)
        upper = upper_model.predict(X_pred)

        # Guarantee lower <= predicted <= upper. Quantile regressors can
        # cross when individual properties are extreme.
        lower = np.minimum(lower, predicted)
        upper = np.maximum(upper, predicted)

        results = pd.DataFrame(
            {
                "property_id": df_all["parcel_number"].astype(str),
                "predicted_value": np.round(predicted, 2),
                "predicted_value_lower": np.round(lower, 2),
                "predicted_value_upper": np.round(upper, 2),
                "predicted_at": pd.Timestamp.now(tz="UTC"),
            }
        )

        write_predictions(client, results)
        upload_feature_importances(median_model, features)

        return (
            f"Loaded {len(results)} predictions to {OUTPUT_TABLE}.",
            200,
        )

    except Exception as exc:
        print(f"Error: {exc}")
        return (f"Error: {exc}", 500)
