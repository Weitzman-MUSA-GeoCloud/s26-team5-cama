# Deployment script for CAMA data pipeline Cloud Functions and Workflow on Windows.
# Run this script from the root of the tasks folder.

# Prerequisites:
#   - gcloud CLI installed and configured.
#   - Authenticated with: gcloud auth login
#   - Project set with: gcloud config set project musa5090s26-team5

# Usage:
#   In PowerShell: .\deploy.ps1

# Configuration variables.
$PROJECT_ID = "musa5090s26-team5"
$REGION = "us-east4"
$RAW_DATA_BUCKET = "musa5090s26-team5-raw_data"
$PREPARED_DATA_BUCKET = "musa5090s26-team5-prepared_data"

Write-Host "Extract Functions" -ForegroundColor Green

# Extract OPA Properties.
Write-Host "Deploying extract-opa-properties."
gcloud functions deploy extract-opa-properties `
    --gen2 `
    --runtime=python311 `
    --region=$REGION `
    --source=tasks/extract_opa_properties `
    --entry-point=extract_opa_properties `
    --trigger-http `
    --set-env-vars RAW_DATA_BUCKET=$RAW_DATA_BUCKET `
    --timeout=1800s `
    --memory=8GB `
    --no-allow-unauthenticated

# Extract OPA Assessments.
Write-Host "Deploying extract-opa-assessments."
gcloud functions deploy extract-opa-assessments `
    --gen2 `
    --runtime=python311 `
    --region=$REGION `
    --source=tasks/extract_opa_assessments `
    --entry-point=extract_opa_assessments `
    --trigger-http `
    --set-env-vars RAW_DATA_BUCKET=$RAW_DATA_BUCKET `
    --timeout=1800s `
    --memory=2GB `
    --no-allow-unauthenticated

# Extract PWD Parcels.
Write-Host "Deploying extract-pwd-parcels."
gcloud functions deploy extract-pwd-parcels `
    --gen2 `
    --runtime=python311 `
    --region=$REGION `
    --source=tasks/extract_pwd_parcels `
    --entry-point=extract_pwd_parcels `
    --trigger-http `
    --set-env-vars RAW_DATA_BUCKET=$RAW_DATA_BUCKET `
    --timeout=1800s `
    --memory=4GB `
    --no-allow-unauthenticated

Write-Host "Prepare Functions" -ForegroundColor Green

# Prepare OPA Properties.
Write-Host "Deploying prepare-opa-properties."
gcloud functions deploy prepare-opa-properties `
    --gen2 `
    --runtime=python311 `
    --region=$REGION `
    --source=tasks/prepare_opa_properties `
    --entry-point=prepare_opa_properties `
    --trigger-http `
    --set-env-vars "RAW_DATA_BUCKET=$RAW_DATA_BUCKET,PREPARED_DATA_BUCKET=$PREPARED_DATA_BUCKET" `
    --timeout=1800s `
    --memory=8GB `
    --no-allow-unauthenticated

# Prepare OPA Assessments.
Write-Host "Deploying prepare-opa-assessments."
gcloud functions deploy prepare-opa-assessments `
    --gen2 `
    --runtime=python311 `
    --region=$REGION `
    --source=tasks/prepare_opa_assessments `
    --entry-point=prepare_opa_assessments `
    --trigger-http `
    --set-env-vars "RAW_DATA_BUCKET=$RAW_DATA_BUCKET,PREPARED_DATA_BUCKET=$PREPARED_DATA_BUCKET" `
    --timeout=1800s `
    --memory=4GB `
    --no-allow-unauthenticated

# Prepare PWD Parcels.
Write-Host "Deploying prepare-pwd-parcels."
gcloud functions deploy prepare-pwd-parcels `
    --gen2 `
    --runtime=python311 `
    --region=$REGION `
    --source=tasks/prepare_pwd_parcels `
    --entry-point=prepare_pwd_parcels `
    --trigger-http `
    --set-env-vars "RAW_DATA_BUCKET=$RAW_DATA_BUCKET,PREPARED_DATA_BUCKET=$PREPARED_DATA_BUCKET" `
    --timeout=1800s `
    --memory=4GB `
    --no-allow-unauthenticated

Write-Host "Load Functions" -ForegroundColor Green

# Load OPA Properties.
Write-Host "Deploying load-opa-properties."
gcloud functions deploy load-opa-properties `
    --gen2 `
    --runtime=python311 `
    --region=$REGION `
    --source=tasks/load_opa_properties `
    --entry-point=load_opa_properties `
    --trigger-http `
    --set-env-vars DATA_LAKE_BUCKET=$PREPARED_DATA_BUCKET `
    --timeout=1800s `
    --memory=512MB `
    --no-allow-unauthenticated

# Load OPA Assessments.
Write-Host "Deploying load-opa-assessments."
gcloud functions deploy load-opa-assessments `
    --gen2 `
    --runtime=python311 `
    --region=$REGION `
    --source=tasks/load_opa_assessments `
    --entry-point=load_opa_assessments `
    --trigger-http `
    --set-env-vars DATA_LAKE_BUCKET=$PREPARED_DATA_BUCKET `
    --timeout=1800s `
    --memory=512MB `
    --no-allow-unauthenticated

# Load PWD Parcels.
Write-Host "Deploying load-pwd-parcels."
gcloud functions deploy load-pwd-parcels `
    --gen2 `
    --runtime=python311 `
    --region=$REGION `
    --source=tasks/load_pwd_parcels `
    --entry-point=load_pwd_parcels `
    --trigger-http `
    --set-env-vars DATA_LAKE_BUCKET=$PREPARED_DATA_BUCKET `
    --timeout=1800s `
    --memory=512MB `
    --no-allow-unauthenticated

Write-Host "Workflow" -ForegroundColor Green

# Deploy the data pipeline workflow.
Write-Host "Deploying data-pipeline workflow."
gcloud workflows deploy data-pipeline `
    --location=$REGION `
    --source=tasks/workflows/data_pipeline.yaml

Write-Host "Done. Now execute workflow manually by typing:" -ForegroundColor Green
Write-Host "  gcloud workflows run data-pipeline --location=$REGION"