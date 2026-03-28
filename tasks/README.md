# Tasks

This folder contains the Cloud Functions and Workflows for the Philadelphia Computer-Assisted Mass Appraisal (CAMA) data pipeline.

## Structure

```
tasks/
├── extract_opa_properties/     # Extract OPA Properties from CARTO API.
│   ├── main.py
│   └── requirements.txt
├── extract_opa_assessments/    # Extract OPA Assessments from OpenData S3.
│   ├── main.py
│   └── requirements.txt
├── extract_pwd_parcels/        # Extract PWD Parcels from ArcGIS Hub.
│   ├── main.py
│   └── requirements.txt
├── prepare_opa_properties/     # Prepare OPA Properties as GeoParquet.
│   ├── main.py
│   └── requirements.txt
├── prepare_opa_assessments/    # Prepare OPA Assessments as Parquet.
│   ├── main.py
│   └── requirements.txt
├── prepare_pwd_parcels/        # Prepare PWD Parcels as GeoParquet.
│   ├── main.py
│   └── requirements.txt
├── load_opa_properties/        # Load OPA Properties into BigQuery.
│   ├── main.py
│   ├── requirements.txt
│   ├── source_opa_properties.sql
│   └── core_opa_properties.sql
├── load_opa_assessments/       # Load OPA Assessments into BigQuery.
│   ├── main.py
│   ├── requirements.txt
│   ├── source_opa_assessments.sql
│   └── core_opa_assessments.sql
├── load_pwd_parcels/           # Load PWD Parcels into BigQuery.
│   ├── main.py
│   ├── requirements.txt
│   ├── source_pwd_parcels.sql
│   └── core_pwd_parcels.sql
├── workflows/
│   └── data_pipeline.yaml      # Orchestration workflow.
├── deploy.ps1                  # PowerShell deployment script.
└── README.md                   # This file.
```

## Cloud Functions

Each Cloud Function has its own folder containing:
- `main.py` - The function code.
- `requirements.txt` - Python dependencies.

### Deployment

Deploy each Cloud Function using the `gcloud` CLI. The functions use default credentials.

```pwsh
# Log in first.
gcloud auth login

# Extract functions.
gcloud functions deploy extract-opa-properties `
    --gen2 `
    --runtime=python311 `
    --region=us-east4 `
    --source=tasks/extract_opa_properties `
    --entry-point=extract_opa_properties `
    --trigger-http `
    --set-env-vars RAW_DATA_BUCKET=musa5090s26-team5-raw_data `
    --timeout=1800s `
    --memory=8GB `
    --no-allow-unauthenticated

gcloud functions deploy extract-opa-assessments `
    --gen2 `
    --runtime=python311 `
    --region=us-east4 `
    --source=tasks/extract_opa_assessments `
    --entry-point=extract_opa_assessments `
    --trigger-http `
    --set-env-vars RAW_DATA_BUCKET=musa5090s26-team5-raw_data `
    --timeout=1800s `
    --memory=2GB `
    --no-allow-unauthenticated

gcloud functions deploy extract-pwd-parcels `
    --gen2 `
    --runtime=python311 `
    --region=us-east4 `
    --source=tasks/extract_pwd_parcels `
    --entry-point=extract_pwd_parcels `
    --trigger-http `
    --set-env-vars RAW_DATA_BUCKET=musa5090s26-team5-raw_data `
    --timeout=1800s `
    --memory=4GB `
    --no-allow-unauthenticated

# Prepare functions.
gcloud functions deploy prepare-opa-properties `
    --gen2 `
    --runtime=python311 `
    --region=us-east4 `
    --source=tasks/prepare_opa_properties `
    --entry-point=prepare_opa_properties `
    --trigger-http `
    --set-env-vars "RAW_DATA_BUCKET=musa5090s26-team5-raw_data,PREPARED_DATA_BUCKET=musa5090s26-team5-prepared_data" `
    --timeout=1800s `
    --memory=8GB `
    --no-allow-unauthenticated

gcloud functions deploy prepare-opa-assessments `
    --gen2 `
    --runtime=python311 `
    --region=us-east4 `
    --source=tasks/prepare_opa_assessments `
    --entry-point=prepare_opa_assessments `
    --trigger-http `
    --set-env-vars "RAW_DATA_BUCKET=musa5090s26-team5-raw_data,PREPARED_DATA_BUCKET=musa5090s26-team5-prepared_data" `
    --timeout=1800s `
    --memory=4GB `
    --no-allow-unauthenticated

gcloud functions deploy prepare-pwd-parcels `
    --gen2 `
    --runtime=python311 `
    --region=us-east4 `
    --source=tasks/prepare_pwd_parcels `
    --entry-point=prepare_pwd_parcels `
    --trigger-http `
    --set-env-vars "RAW_DATA_BUCKET=musa5090s26-team5-raw_data,PREPARED_DATA_BUCKET=musa5090s26-team5-prepared_data" `
    --timeout=1800s `
    --memory=4GB `
    --no-allow-unauthenticated

# Load functions.
gcloud functions deploy load-opa-properties `
    --gen2 `
    --runtime=python311 `
    --region=us-east4 `
    --source=tasks/load_opa_properties `
    --entry-point=load_opa_properties `
    --trigger-http `
    --set-env-vars DATA_LAKE_BUCKET=musa5090s26-team5-prepared_data `
    --timeout=1800s `
    --memory=512MB `
    --no-allow-unauthenticated

gcloud functions deploy load-opa-assessments `
    --gen2 `
    --runtime=python311 `
    --region=us-east4 `
    --source=tasks/load_opa_assessments `
    --entry-point=load_opa_assessments `
    --trigger-http `
    --set-env-vars DATA_LAKE_BUCKET=musa5090s26-team5-prepared_data `
    --timeout=1800s `
    --memory=512MB `
    --no-allow-unauthenticated

gcloud functions deploy load-pwd-parcels `
    --gen2 `
    --runtime=python311 `
    --region=us-east4 `
    --source=tasks/load_pwd_parcels `
    --entry-point=load_pwd_parcels `
    --trigger-http `
    --set-env-vars DATA_LAKE_BUCKET=musa5090s26-team5-prepared_data `
    --timeout=1800s `
    --memory=512MB `
    --no-allow-unauthenticated
```

## Workflow

Deploy the data pipeline workflow:

```pwsh
gcloud workflows deploy data-pipeline `
    --location=us-east4 `
    --source=tasks/workflows/data_pipeline.yaml
```

Execute the workflow manually:

```pwsh
gcloud workflows run data-pipeline --location=us-east4
```

Individual workflow runs:

```pwsh
# Extract functions.
gcloud functions call extract-opa-properties --region=us-east4
gcloud functions call extract-opa-assessments --region=us-east4
gcloud functions call extract-pwd-parcels --region=us-east4

# Prepare functions.
gcloud functions call prepare-opa-properties --region=us-east4
gcloud functions call prepare-opa-assessments --region=us-east4
gcloud functions call prepare-pwd-parcels --region=us-east4

# Load functions.
gcloud functions call load-opa-properties --region=us-east4
gcloud functions call load-opa-assessments --region=us-east4
gcloud functions call load-pwd-parcels --region=us-east4
```

Scheduler:

```pwsh
gcloud scheduler jobs create http workflow-weekly-monday-6am `
    --location="us-east4" `
    --schedule="0 6 * * 1" `
    --time-zone="America/New_York" `
    --uri="https://workflowexecutions.googleapis.com/v1/projects/musa5090s26-team5/locations/us-east4/workflows/data-pipeline/executions" `
    --http-method=POST `
    --oidc-service-account-email=data-pipeline-user@musa5090s26-team5.iam.gserviceaccount.com `
    --oidc-token-audience="https://workflowexecutions.googleapis.com" `
    --headers="Content-Type=application/json" `
    --message-body='{}'
```

## Prerequisites

Before deploying, ensure the following GCP resources exist:

1. **Cloud Storage Buckets:**
   - `musa5090s26-team5-raw_data`
   - `musa5090s26-team5-prepared_data`

2. **BigQuery Datasets:**
   - `source` - For external tables.
   - `core` - For internal tables.

## Data Flow

```
┌─────────────┐      ┌─────────────┐     ┌─────────────┐
│   Extract   │────▶│   Prepare   │────▶│    Load     │
└─────────────┘      └─────────────┘     └─────────────┘
       │                  │                   │
       ▼                  ▼                   ▼
  raw_data/          prepared_data/       BigQuery
  *.csv, *.jsonl      *.parquet         source.* & core.*
```
