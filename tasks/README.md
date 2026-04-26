# Tasks

This folder contains the Cloud Functions and Workflows for the Philadelphia Computer-Assisted Mass Appraisal (CAMA) data pipeline.

## Structure

```
tasks/
├── config-cors/                       # CORS configuration for the public bucket.
│   └── cors.json
├── create_training_data/              # Create model training data in BigQuery.
│   ├── create_training_data.sql
│   ├── main.py
│   └── requirements.txt
├── current_assessment_bins/           # Create current (predicted) assessment bins in BigQuery.
│   ├── current_assessment_bins.sql
│   ├── main.py
│   └── requirements.txt
├── export_property_tile_info/         # Export a GeoJSON file of assessment values for each property.
│   ├── main.py
│   ├── property_tile_info.sql
│   └── requirements.txt
├── extract_neighborhoods/             # Extract Philadelphia Neighborhoods as Parquet.
│   ├── main.py
│   └── requirements.txt
├── extract_opa_assessments/           # Extract OPA Assessments from OpenData S3.
│   ├── main.py
│   └── requirements.txt
├── extract_opa_properties/            # Extract OPA Properties from CARTO API.
│   ├── main.py
│   └── requirements.txt
├── extract_pwd_parcels/               # Extract PWD Parcels from ArcGIS Hub.
│   ├── main.py
│   └── requirements.txt
├── extract_septa/                     # Extract SEPTA Stations as CSV.
│   ├── main.py
│   └── requirements.txt
├── generate_assessment_chart_config/  # Generate predicted-assessment chart config JSON for front-end.
│   ├── main.py
│   └── requirements.txt
├── generate_map_styling_metadata/     # Export map styling metadata JSON for front-end choropleths.
│   ├── main.py
│   └── requirements.txt
├── generate_property_map_tiles/       # Cloud Run job that builds vector tiles from the property GeoJSON.
│   ├── Dockerfile
│   └── script.sh
├── generate_tax_year_chart_config/    # Generate tax-year-assessment chart config JSON for front-end.
│   ├── main.py
│   └── requirements.txt
├── load_neighborhoods/                # Load Philadelphia Neighborhoods into BigQuery.
│   ├── core_neighborhoods.sql
│   ├── main.py
│   ├── requirements.txt
│   └── source_neighborhoods.sql
├── load_opa_assessments/              # Load OPA Assessments into BigQuery.
│   ├── core_opa_assessments.sql
│   ├── main.py
│   ├── requirements.txt
│   └── source_opa_assessments.sql
├── load_opa_properties/               # Load OPA Properties into BigQuery.
│   ├── core_opa_properties.sql
│   ├── main.py
│   ├── requirements.txt
│   └── source_opa_properties.sql
├── load_pwd_parcels/                  # Load PWD Parcels into BigQuery.
│   ├── core_pwd_parcels.sql
│   ├── main.py
│   ├── requirements.txt
│   └── source_pwd_parcels.sql
├── load_septa/                        # Load SEPTA Stations into BigQuery.
│   ├── core_septa.sql
│   ├── main.py
│   ├── requirements.txt
│   └── source_septa.sql
├── prepare_neighborhoods/             # Prepare Philadelphia Neighborhoods as Parquet.
│   ├── main.py
│   └── requirements.txt
├── prepare_opa_assessments/           # Prepare OPA Assessments as Parquet.
│   ├── main.py
│   └── requirements.txt
├── prepare_opa_properties/            # Prepare OPA Properties as GeoParquet.
│   ├── main.py
│   └── requirements.txt
├── prepare_pwd_parcels/               # Prepare PWD Parcels as GeoParquet.
│   ├── main.py
│   └── requirements.txt
├── prepare_septa/                     # Prepare SEPTA Stations as Parquet.
│   ├── main.py
│   └── requirements.txt
├── tax_year_bins/                     # Create tax-year assessment bins in BigQuery.
│   ├── main.py
│   ├── requirements.txt
│   └── tax_year_assessment_bins.sql
├── workflows/
│   └── data_pipeline.yaml             # Orchestration workflow.
├── deploy.ps1                         # PowerShell deployment script.
└── README.md                          # This file.
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

# Establish CORS configuration for musa5090s26-team5-public bucket.
gcloud storage buckets update gs://musa5090s26-team5-public --cors-file=tasks/config-cors/cors.json

# Validate the CORS json is in the public bucket.
gcloud storage buckets describe gs://musa5090s26-team5-public

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

gcloud functions deploy extract-neighborhoods `
    --gen2 `
    --runtime=python311 `
    --region=us-east4 `
    --source=tasks/extract_neighborhoods `
    --entry-point=extract_neighborhoods `
    --trigger-http `
    --set-env-vars RAW_DATA_BUCKET=musa5090s26-team5-raw_data `
    --timeout=1800s `
    --memory=512MB `
    --no-allow-unauthenticated

gcloud functions deploy extract-septa `
    --gen2 `
    --runtime=python311 `
    --region=us-east4 `
    --source=tasks/extract_septa `
    --entry-point=extract_septa `
    --trigger-http `
    --set-env-vars RAW_DATA_BUCKET=musa5090s26-team5-raw_data `
    --timeout=1800s `
    --memory=512MB `
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

gcloud functions deploy prepare-neighborhoods `
    --gen2 `
    --runtime=python311 `
    --region=us-east4 `
    --source=tasks/prepare_neighborhoods `
    --entry-point=prepare_neighborhoods `
    --trigger-http `
    --set-env-vars "RAW_DATA_BUCKET=musa5090s26-team5-raw_data,PREPARED_DATA_BUCKET=musa5090s26-team5-prepared_data" `
    --timeout=1800s `
    --memory=512MB `
    --no-allow-unauthenticated

gcloud functions deploy prepare-septa `
    --gen2 `
    --runtime=python311 `
    --region=us-east4 `
    --source=tasks/prepare_septa `
    --entry-point=prepare_septa `
    --trigger-http `
    --set-env-vars "RAW_DATA_BUCKET=musa5090s26-team5-raw_data,PREPARED_DATA_BUCKET=musa5090s26-team5-prepared_data" `
    --timeout=1800s `
    --memory=512MB `
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

gcloud functions deploy load-neighborhoods `
    --gen2 `
    --runtime=python311 `
    --region=us-east4 `
    --source=tasks/load_neighborhoods `
    --entry-point=load_neighborhoods `
    --trigger-http `
    --set-env-vars DATA_LAKE_BUCKET=musa5090s26-team5-prepared_data `
    --timeout=1800s `
    --memory=512MB `
    --no-allow-unauthenticated

gcloud functions deploy load-septa `
    --gen2 `
    --runtime=python311 `
    --region=us-east4 `
    --source=tasks/load_septa `
    --entry-point=load_septa `
    --trigger-http `
    --set-env-vars DATA_LAKE_BUCKET=musa5090s26-team5-prepared_data `
    --timeout=1800s `
    --memory=512MB `
    --no-allow-unauthenticated

# Derived functions.
gcloud functions deploy create-training-data `
    --gen2 `
    --runtime=python311 `
    --region=us-east4 `
    --source=tasks/create_training_data `
    --entry-point=create_training_data `
    --trigger-http `
    --timeout=1800s `
    --memory=512MB `
    --no-allow-unauthenticated

gcloud functions deploy create-tax-year-assessment-bins `
    --gen2 `
    --runtime=python311 `
    --region=us-east4 `
    --source=tasks/tax_year_bins `
    --entry-point=create_tax_year_assessment_bins `
    --trigger-http `
    --timeout=1800s `
    --memory=512MB `
    --no-allow-unauthenticated

gcloud functions deploy generate-tax-year-chart-config `
    --gen2 `
    --runtime=python311 `
    --region=$REGION `
    --source=tasks/generate_tax_year_chart_config `
    --entry-point=generate_tax_year_chart_config `
    --trigger-http `
    --timeout=1800s `
    --memory=512MB `
    --no-allow-unauthenticated

gcloud functions deploy export-property-tile-info `
    --gen2 `
    --runtime=python311 `
    --region=$REGION `
    --source=tasks/export_property_tile_info `
    --entry-point=export_property_tile_info `
    --trigger-http `
    --timeout=1800s `
    --memory=4GB `
    --no-allow-unauthenticated

gcloud functions deploy generate-map-styling-metadata `
    --gen2 `
    --runtime=python311 `
    --region=$REGION `
    --source=tasks/generate_map_styling_metadata `
    --entry-point=generate_map_styling_metadata `
    --trigger-http `
    --timeout=1800s `
    --memory=512MB `
    --no-allow-unauthenticated

```

## Cloud Run Jobs

`generate_property_map_tiles` is a Cloud Run job (not a Cloud Function). It downloads the property GeoJSON from Cloud Storage, converts it to vector tiles using `ogr2ogr`, and uploads the tiles to the public bucket.

### Deployment

Create the Artifact Registry repository (if it doesn't exist), build the container image, and deploy the job:

```pwsh
# Create the Artifact Registry repository for Docker images.
gcloud artifacts repositories create cama `
    --repository-format=docker `
    --location=us-east4 `
    --quiet || echo "Repository already exists."

# Build the container image with Cloud Build.
gcloud builds submit tasks/generate_property_map_tiles `
    --tag=us-east4-docker.pkg.dev/musa5090s26-team5/cama/generate-property-map-tiles

# Deploy the Cloud Run job.
gcloud run jobs deploy generate-property-map-tiles `
    --image=us-east4-docker.pkg.dev/musa5090s26-team5/cama/generate-property-map-tiles `
    --region=us-east4
```

### Execution

```pwsh
gcloud run jobs execute generate-property-map-tiles --region=us-east4 --wait
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
gcloud functions call extract-neighborhoods --region=us-east4
gcloud functions call extract-septa --region=us-east4

# Prepare functions.
gcloud functions call prepare-opa-properties --region=us-east4
gcloud functions call prepare-opa-assessments --region=us-east4
gcloud functions call prepare-pwd-parcels --region=us-east4
gcloud functions call prepare-neighborhoods --region=us-east4
gcloud functions call prepare-septa --region=us-east4

# Load functions.
gcloud functions call load-opa-properties --region=us-east4
gcloud functions call load-opa-assessments --region=us-east4
gcloud functions call load-pwd-parcels --region=us-east4
gcloud functions call load-neighborhoods --region=us-east4
gcloud functions call load-septa --region=us-east4

# Derived functions.
gcloud functions call create-training-data --region=us-east4
gcloud functions call create-tax-year-assessment-bins --region=us-east4
gcloud functions call generate-tax-year-chart-config --region=us-east4
gcloud functions call export-property-tile-info --region=us-east4

# Generate property map tiles (Cloud Run job).
gcloud run jobs execute generate-property-map-tiles --region=us-east4 --wait
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
   - `derived` - For derived/aggregated tables.

## Data Flow

```
┌─────────────┐      ┌─────────────┐     ┌─────────────┐      ┌─────────────┐
│   Extract   │────▶│   Prepare   │────▶│    Load     │────▶│   Derived   │
└─────────────┘      └─────────────┘     └─────────────┘      └─────────────┘
       │                  │                   │                    │
       ▼                  ▼                   ▼                    ▼
  raw_data/          prepared_data/       BigQuery             BigQuery
  *.csv, *.jsonl      *.parquet         source.* & core.*    derived.*
```
