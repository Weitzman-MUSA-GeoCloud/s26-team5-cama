"""
Evaluate the production CAMA sale-price model.

Mirrors `tasks/run_current_assessments_model/main.py` exactly — same
features, same GradientBoostingRegressor hyperparameters, same outlier
filtering, same three quantile models — but adds an 80/20 train/test
split so we can report real evaluation metrics.

Outputs (all written to ./eval_output/ next to this script):
  - metrics.json          Machine-readable summary of every metric below
  - metrics_summary.md    Human-readable summary, paste-ready for the form
  - feature_importance.png  Bar chart for the code walkthrough
  - feature_importance.csv  Same data in CSV

Metrics reported:
  - Test MAE, Train MAE, train/test gap (overfit diagnostic)
  - Test R²
  - Median Absolute Percentage Error (MAPE) — more interpretable than MAE
    for a target with a wide range of magnitudes
  - 80% prediction interval empirical coverage (should be ~0.80)
  - Mean interval width as % of predicted value (interval informativeness)
  - Feature importances ranked

Run from the repo root in the `base` conda environment after
`gcloud auth application-default login`:

    python eda/evaluate_production_model.py
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # noqa: E402  no display needed
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from google.cloud import bigquery  # noqa: E402
from sklearn.ensemble import GradientBoostingRegressor  # noqa: E402
from sklearn.metrics import mean_absolute_error, r2_score  # noqa: E402
from sklearn.model_selection import train_test_split  # noqa: E402
from sklearn.preprocessing import LabelEncoder  # noqa: E402

# -----------------------------------------------------------------------------
# Mirror production main.py exactly
# -----------------------------------------------------------------------------

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "musa5090s26-team5")

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

GBR_KWARGS = dict(
    n_estimators=200,
    max_depth=5,
    learning_rate=0.1,
    random_state=42,
)

OUTLIER_QUANTILE = 0.99
TEST_SIZE = 0.2
SPLIT_RANDOM_STATE = 42

OUTPUT_DIR = Path(__file__).resolve().parent / "eval_output"
OUTPUT_DIR.mkdir(exist_ok=True)


# -----------------------------------------------------------------------------
# Data loading
# -----------------------------------------------------------------------------

def load_training_data(client: bigquery.Client) -> pd.DataFrame:
    """Pull training data from derived.current_assessments_model_training_data."""
    print(f"Loading training data from {PROJECT_ID}.derived...")
    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.derived.current_assessments_model_training_data`
    """
    df = client.query(query).to_dataframe(create_bqstorage_client=False)
    print(f"Loaded {len(df):,} rows.")
    return df


def prepare_frame(df: pd.DataFrame, label_encoders=None, fit: bool = False):
    """Encode categoricals + impute numerics. Same logic as production."""
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


# -----------------------------------------------------------------------------
# Evaluation
# -----------------------------------------------------------------------------

def evaluate(df: pd.DataFrame) -> dict:
    target_col = "sale_price"

    # 1. Outlier filter — same as production
    n_before = len(df)
    q99 = df[target_col].quantile(OUTLIER_QUANTILE)
    df = df[df[target_col] <= q99].copy()
    n_after = len(df)
    print(f"Outlier filter: kept {n_after:,}/{n_before:,} rows (sale_price <= ${q99:,.0f}).")

    # 2. Encode + impute (fit encoders on full set so train/test share encoding)
    X_all, label_encoders = prepare_frame(df[FEATURE_COLS], fit=True)
    y_all = pd.to_numeric(df[target_col], errors="coerce")
    keep = y_all.notna()
    X_all = X_all[keep]
    y_all = y_all[keep]

    # 3. 80/20 train/test split
    X_train, X_test, y_train, y_test = train_test_split(
        X_all, y_all,
        test_size=TEST_SIZE,
        random_state=SPLIT_RANDOM_STATE,
    )
    print(f"Train: {len(X_train):,}  Test: {len(X_test):,}")

    # 4. Train three models — same hyperparameters as production
    print("Training main model (least-squares loss)...")
    model_main = GradientBoostingRegressor(**GBR_KWARGS)
    model_main.fit(X_train, y_train)

    print("Training lower-quantile model (alpha=0.1)...")
    model_lower = GradientBoostingRegressor(loss="quantile", alpha=0.1, **GBR_KWARGS)
    model_lower.fit(X_train, y_train)

    print("Training upper-quantile model (alpha=0.9)...")
    model_upper = GradientBoostingRegressor(loss="quantile", alpha=0.9, **GBR_KWARGS)
    model_upper.fit(X_train, y_train)

    # 5. Predict on both splits
    y_pred_train = model_main.predict(X_train)
    y_pred_test = model_main.predict(X_test)
    y_lower_test = model_lower.predict(X_test)
    y_upper_test = model_upper.predict(X_test)

    # Clamp so lower <= main <= upper (same as production)
    y_lower_test = np.minimum(y_lower_test, y_pred_test)
    y_upper_test = np.maximum(y_upper_test, y_pred_test)

    # 6. Metrics — main model
    mae_train = float(mean_absolute_error(y_train, y_pred_train))
    mae_test = float(mean_absolute_error(y_test, y_pred_test))
    gap = mae_test - mae_train
    r2_test = float(r2_score(y_test, y_pred_test))

    # Median absolute percentage error — robust to skew, easier to interpret
    pct_err = np.abs(y_test.values - y_pred_test) / np.maximum(y_test.values, 1.0)
    mdape = float(np.median(pct_err))
    mape_mean = float(np.mean(pct_err))

    # 7. Metrics — prediction interval (80% nominal: q=0.1 to q=0.9)
    in_interval = (y_test.values >= y_lower_test) & (y_test.values <= y_upper_test)
    coverage = float(np.mean(in_interval))

    interval_width = y_upper_test - y_lower_test
    rel_width = interval_width / np.maximum(y_pred_test, 1.0)
    median_rel_width = float(np.median(rel_width))
    mean_rel_width = float(np.mean(rel_width))

    # 8. Overfit diagnostic — staged predictions
    print("Computing staged predictions for overfit curve...")
    train_maes_staged = []
    test_maes_staged = []
    for stage_pred_train, stage_pred_test in zip(
        model_main.staged_predict(X_train),
        model_main.staged_predict(X_test),
    ):
        train_maes_staged.append(float(mean_absolute_error(y_train, stage_pred_train)))
        test_maes_staged.append(float(mean_absolute_error(y_test, stage_pred_test)))
    best_n = int(np.argmin(test_maes_staged)) + 1

    # 9. Feature importances
    importances = pd.Series(
        model_main.feature_importances_,
        index=FEATURE_COLS,
    ).sort_values(ascending=False)

    # ----- Outputs -----

    # Feature importance CSV + PNG
    importances.to_csv(OUTPUT_DIR / "feature_importance.csv", header=["importance"])

    fig, ax = plt.subplots(figsize=(8, 6))
    importances.sort_values().plot.barh(ax=ax, color="#3b6fb6")
    ax.set_xlabel("Relative importance")
    ax.set_title("Feature importance — production sale-price model")
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "feature_importance.png", dpi=150)
    plt.close(fig)

    # Overfit curve PNG
    fig, ax = plt.subplots(figsize=(9, 4))
    ax.plot(train_maes_staged, label="Train MAE", alpha=0.85)
    ax.plot(test_maes_staged, label="Test MAE", alpha=0.85)
    ax.axvline(best_n - 1, color="red", linestyle="--",
               label=f"Best ({best_n} trees)")
    ax.set_xlabel("n_estimators")
    ax.set_ylabel("MAE ($)")
    ax.set_title("Train vs test MAE — overfit diagnostic")
    ax.legend()
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "overfit_curve.png", dpi=150)
    plt.close(fig)

    # Metrics dict
    metrics = {
        "evaluated_at_utc": datetime.now(timezone.utc).isoformat(),
        "n_rows_loaded": int(n_before),
        "n_rows_after_outlier_filter": int(n_after),
        "outlier_cap_sale_price": float(q99),
        "n_train": int(len(X_train)),
        "n_test": int(len(X_test)),
        "main_model": {
            "test_mae_dollars": mae_test,
            "train_mae_dollars": mae_train,
            "train_test_gap_dollars": float(gap),
            "test_r2": r2_test,
            "median_abs_pct_error": mdape,
            "mean_abs_pct_error": mape_mean,
            "best_n_estimators_by_test_mae": best_n,
        },
        "prediction_interval_80pct": {
            "empirical_coverage": coverage,
            "median_relative_width": median_rel_width,
            "mean_relative_width": mean_rel_width,
        },
        "feature_importances": [
            {"feature": feat, "importance": float(imp)}
            for feat, imp in importances.items()
        ],
    }

    with open(OUTPUT_DIR / "metrics.json", "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

    # Human-readable summary
    summary = f"""# Production Model Evaluation

_Generated {metrics['evaluated_at_utc']}_

Same features, hyperparameters, and outlier filter as
`tasks/run_current_assessments_model/main.py` (commit `4ad7645`).
Difference: 80/20 train/test split is added so we can report metrics.

## Data

- Rows loaded from `derived.current_assessments_model_training_data`: **{n_before:,}**
- Outlier filter (drop top 1% sale_price > ${q99:,.0f}): **{n_after:,}** rows kept
- Train: **{len(X_train):,}**  |  Test: **{len(X_test):,}**

## Main model — `GradientBoostingRegressor` (least-squares loss)

| Metric | Value |
|---|---|
| Test MAE | **${mae_test:,.0f}** |
| Train MAE | ${mae_train:,.0f} |
| Train/test gap (overfit) | ${gap:,.0f} |
| Test R² | **{r2_test:.4f}** |
| Median absolute % error (MdAPE) | **{mdape:.1%}** |
| Mean absolute % error | {mape_mean:.1%} |
| Best `n_estimators` by test MAE | {best_n} (we use 200) |

## Prediction interval — 80% nominal

We train two extra GBR models with `loss="quantile"`, alpha=0.1 and
alpha=0.9, to bound each prediction.

| Metric | Value |
|---|---|
| Empirical coverage on test | **{coverage:.1%}** (target 80%) |
| Median interval width / predicted value | {median_rel_width:.1%} |
| Mean interval width / predicted value | {mean_rel_width:.1%} |

## Top 10 features by importance

"""
    summary += "| Rank | Feature | Importance |\n|---|---|---|\n"
    for i, (feat, imp) in enumerate(importances.head(10).items(), 1):
        summary += f"| {i} | `{feat}` | {imp:.4f} |\n"

    summary += "\n_Full ranking in `feature_importance.csv`._\n"

    with open(OUTPUT_DIR / "metrics_summary.md", "w", encoding="utf-8") as f:
        f.write(summary)

    return metrics


# -----------------------------------------------------------------------------
# Entry point
# -----------------------------------------------------------------------------

def main() -> int:
    print(f"Output directory: {OUTPUT_DIR}")
    client = bigquery.Client(project=PROJECT_ID)
    df = load_training_data(client)
    metrics = evaluate(df)

    # Print key numbers to stdout for quick copy-paste
    main_m = metrics["main_model"]
    pi = metrics["prediction_interval_80pct"]
    print()
    print("=" * 60)
    print("SUMMARY (paste-ready for the form)")
    print("=" * 60)
    print(f"  Test MAE:            ${main_m['test_mae_dollars']:,.0f}")
    print(f"  Test R²:             {main_m['test_r2']:.4f}")
    print(f"  Median abs % error:  {main_m['median_abs_pct_error']:.1%}")
    print(f"  Train/test gap:      ${main_m['train_test_gap_dollars']:,.0f}")
    print(f"  80% PI coverage:     {pi['empirical_coverage']:.1%}")
    print(f"  Median rel. PI width: {pi['median_relative_width']:.1%}")
    print()
    print(f"Wrote: {OUTPUT_DIR}/metrics.json")
    print(f"Wrote: {OUTPUT_DIR}/metrics_summary.md")
    print(f"Wrote: {OUTPUT_DIR}/feature_importance.png")
    print(f"Wrote: {OUTPUT_DIR}/feature_importance.csv")
    print(f"Wrote: {OUTPUT_DIR}/overfit_curve.png")
    return 0


if __name__ == "__main__":
    sys.exit(main())
