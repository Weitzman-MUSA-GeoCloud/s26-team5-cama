# run_current_assessments_model

Cloud Function that trains a property valuation model at runtime and writes predictions to BigQuery.

## What it does

On each invocation:

1. Pulls training data from `derived.current_assessments_model_training_data`
2. Trains three `GradientBoostingRegressor` models:
   - **Main model** (least-squares) → `predicted_value`
   - **Lower quantile** (α = 0.1) → `predicted_value_lower`
   - **Upper quantile** (α = 0.9) → `predicted_value_upper`
3. Predicts on all residential properties in `core.opa_properties`
4. Writes results to `derived.current_assessments` (WRITE_TRUNCATE)
5. Uploads feature importances JSON to GCS for the reviewer UI explainability panel

## Output table: `derived.current_assessments`

| Column | Type | Description |
|---|---|---|
| `property_id` | `STRING` | OPA parcel number |
| `predicted_value` | `FLOAT64` | Predicted current market value |
| `predicted_value_lower` | `FLOAT64` | Lower bound of 80% prediction interval |
| `predicted_value_upper` | `FLOAT64` | Upper bound of 80% prediction interval |
| `predicted_at` | `TIMESTAMP` | When the prediction was run |

The table is overwritten (`WRITE_TRUNCATE`) on each run, preserving a history of how predictions change over time by comparing runs.

## Features used

All features match the columns produced by `tasks/create_training_data/create_training_data.sql`.

| Feature | Label |
|---|---|
| `total_livable_area` | Living area (sqft) |
| `total_area` | Total lot area (sqft) |
| `year_built` | Year built |
| `property_age` | Property age (years) |
| `exterior_condition` | Exterior condition |
| `interior_condition` | Interior condition |
| `condition_score` | Avg condition score |
| `number_of_bedrooms` | Bedrooms |
| `number_of_bathrooms` | Bathrooms |
| `zoning` | Zoning |
| `assessed_value_2023` | Assessed value 2023 |
| `assessed_value_2024` | Assessed value 2024 |
| `assessed_value_2025` | Assessed value 2025 |
| `pct_change_2023_to_2025` | % change 2023→2025 |
| `lot_area_sqft` | Lot area (sqft) |
| `lot_perimeter` | Lot perimeter (ft) |
| `lot_shape_ratio` | Lot shape ratio |
| `dist_to_septa_miles` | Distance to SEPTA (mi) |
| `neighborhood` | Neighborhood |

## GCS output

Feature importances are uploaded to:
```
gs://musa5090s26-team5-public/configs/model_feature_importances.json
```

Shape:
```json
{
  "model": "GradientBoostingRegressor",
  "trained_at": "2026-04-27T...",
  "features": [
    {"feature": "total_livable_area", "label": "Living area (sqft)", "importance": 0.35},
    ...
  ]
}
```

## Pipeline position

This function runs after `create-training-data` and before `current-assessment-bins`, `generate-assessment-chart-config`, and `export-property-tile-info`. It is scheduled weekly so the model retrains on the latest sales data.

## Retraining checklist

- [ ] `derived.current_assessments_model_training_data` is up to date (upstream `create-training-data` has run)
- [ ] Cloud Function has `--memory=8GB --cpu=2` (set in `deploy.ps1`)
- [ ] `derived.current_assessments` schema matches the five columns above
- [ ] Feature importances JSON is accessible at the GCS path above
