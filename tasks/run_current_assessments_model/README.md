# predict_assessments

Cloud Function that scores the trained property valuation model on every
residential property in Philadelphia and writes the predictions back to
BigQuery.

Issue #12 (deployment of the Issue #11 model).

## What it does

1. Loads the model bundle from `model.pkl` (a copy of `eda/model.pkl`,
   placed here at deploy time -- see `../deploy.ps1`).
2. Reads features for all residential properties from BigQuery via
   `predict_assessments.sql` (mirrors the feature derivation in
   `../create_training_data/create_training_data.sql`).
3. Applies the same preprocessing the training notebook applied:
   imputes missing values from `bundle["medians"]`, looks up
   `neighborhood_median_price` from `bundle["neighborhood_price_map"]`
   (with `bundle["global_price_fallback"]` for unseen neighborhoods),
   and label-encodes `neighborhood` using
   `bundle["label_encoders"]["neighborhood"]`.
4. Runs `bundle["model"].predict()`.
5. Overwrites `derived.current_assessments` (`property_id`,
   `predicted_value`, `predicted_at`).

## Model bundle contract

`model.pkl` is a dict with these keys:

| Key | Type | Purpose |
|---|---|---|
| `model` | `GradientBoostingRegressor` | The trained estimator. |
| `label_encoders` | `dict[str, LabelEncoder]` | Encoders for categorical features (currently only `neighborhood`). |
| `medians` | `dict[str, float]` | Per-feature imputation values from training. |
| `neighborhood_price_map` | `dict[str, float]` | Median sale price per neighborhood from training. |
| `global_price_fallback` | `float` | Median sale price across all training rows; used when a property's neighborhood is missing or unseen. |

`bundle["model"].feature_names_in_` is the canonical feature order:

```
total_livable_area, number_of_bathrooms, property_age, interior_condition,
assessed_value_2023, assessed_value_2024, assessed_value_2025,
neighborhood, sale_year, neighborhood_median_price
```

`FEATURE_COLS` in `main.py` is kept in sync with this. **If the model is
retrained with a different feature set, update `FEATURE_COLS` and the SQL
together.**

## Deployment

```pwsh
# From repo root.
gcloud functions deploy predict-assessments `
    --gen2 `
    --runtime=python311 `
    --region=us-east4 `
    --source=tasks/predict_assessments `
    --entry-point=predict_assessments `
    --trigger-http `
    --timeout=1800s `
    --memory=8GB `
    --no-allow-unauthenticated
```

`tasks/deploy.ps1` does this and also copies `eda/model.pkl` into this
directory before deploying. `model.pkl` here is gitignored to keep
`eda/model.pkl` as the single source of truth.

## Manual invocation

```pwsh
gcloud workflows run data-pipeline --location=us-east4
# or to invoke just this function:
gcloud functions call predict-assessments --region=us-east4
```

## Notes for retraining

If you retrain the model in `eda/train_model.ipynb`:

1. Save the same bundle structure (`model`, `label_encoders`, `medians`,
   `neighborhood_price_map`, `global_price_fallback`) to `eda/model.pkl`.
2. If the feature set changed, update `FEATURE_COLS` in `main.py` and the
   `SELECT` clause of `predict_assessments.sql` to match.
3. If the sklearn version changed, update the pin in `requirements.txt`.
4. Redeploy: `./tasks/deploy.ps1` (or just the predict-assessments block).
