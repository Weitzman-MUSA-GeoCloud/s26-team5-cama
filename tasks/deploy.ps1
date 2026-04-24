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

# Extract Neighborhoods.
Write-Host "Deploying extract-neighborhoods."
gcloud functions deploy extract-neighborhoods `
    --gen2 `
    --runtime=python311 `
    --region=$REGION `
    --source=tasks/extract_neighborhoods `
    --entry-point=extract_neighborhoods `
    --trigger-http `
    --set-env-vars RAW_DATA_BUCKET=$RAW_DATA_BUCKET `
    --timeout=1800s `
    --memory=512MB `
    --no-allow-unauthenticated

# Extract SEPTA Stations.
Write-Host "Deploying extract-septa."
gcloud functions deploy extract-septa `
    --gen2 `
    --runtime=python311 `
    --region=$REGION `
    --source=tasks/extract_septa `
    --entry-point=extract_septa `
    --trigger-http `
    --set-env-vars RAW_DATA_BUCKET=$RAW_DATA_BUCKET `
    --timeout=1800s `
    --memory=512MB `
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

# Prepare Neighborhoods.
Write-Host "Deploying prepare-neighborhoods."
gcloud functions deploy prepare-neighborhoods `
    --gen2 `
    --runtime=python311 `
    --region=$REGION `
    --source=tasks/prepare_neighborhoods `
    --entry-point=prepare_neighborhoods `
    --trigger-http `
    --set-env-vars "RAW_DATA_BUCKET=$RAW_DATA_BUCKET,PREPARED_DATA_BUCKET=$PREPARED_DATA_BUCKET" `
    --timeout=1800s `
    --memory=512MB `
    --no-allow-unauthenticated

# Prepare SEPTA Stations.
Write-Host "Deploying prepare-septa."
gcloud functions deploy prepare-septa `
    --gen2 `
    --runtime=python311 `
    --region=$REGION `
    --source=tasks/prepare_septa `
    --entry-point=prepare_septa `
    --trigger-http `
    --set-env-vars "RAW_DATA_BUCKET=$RAW_DATA_BUCKET,PREPARED_DATA_BUCKET=$PREPARED_DATA_BUCKET" `
    --timeout=1800s `
    --memory=512MB `
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

# Load Neighborhoods.
Write-Host "Deploying load-neighborhoods."
gcloud functions deploy load-neighborhoods `
    --gen2 `
    --runtime=python311 `
    --region=$REGION `
    --source=tasks/load_neighborhoods `
    --entry-point=load_neighborhoods `
    --trigger-http `
    --set-env-vars DATA_LAKE_BUCKET=$PREPARED_DATA_BUCKET `
    --timeout=1800s `
    --memory=512MB `
    --no-allow-unauthenticated

# Load SEPTA Stations.
Write-Host "Deploying load-septa."
gcloud functions deploy load-septa `
    --gen2 `
    --runtime=python311 `
    --region=$REGION `
    --source=tasks/load_septa `
    --entry-point=load_septa `
    --trigger-http `
    --set-env-vars DATA_LAKE_BUCKET=$PREPARED_DATA_BUCKET `
    --timeout=1800s `
    --memory=512MB `
    --no-allow-unauthenticated

Write-Host "Derived Functions" -ForegroundColor Green

# Create Training Data.
Write-Host "Deploying create-training-data."
gcloud functions deploy create-training-data `
    --gen2 `
    --runtime=python311 `
    --region=$REGION `
    --source=tasks/create_training_data `
    --entry-point=create_training_data `
    --trigger-http `
    --timeout=1800s `
    --memory=512MB `
    --no-allow-unauthenticated

# Tax Year Assessment Bins.
Write-Host "Deploying create-tax-year-assessment-bins."
gcloud functions deploy create-tax-year-assessment-bins `
    --gen2 `
    --runtime=python311 `
    --region=$REGION `
    --source=tasks/tax_year_bins `
    --entry-point=create_tax_year_assessment_bins `
    --trigger-http `
    --timeout=1800s `
    --memory=512MB `
    --no-allow-unauthenticated

# Assessment Chart Config.
Write-Host "Deploying generate-assessment-chart-configs."
gcloud functions deploy generate-assessment-chart-configs `
    --gen2 `
    --runtime=python311 `
    --region=$REGION `
    --source=tasks/generate_assessment_chart_configs `
    --entry-point=generate_assessment_chart_config `
    --trigger-http `
    --timeout=1800s `
    --memory=512MB `
    --no-allow-unauthenticated

# Tax Year Chart Config.
Write-Host " Deploying generate-tax-year-chart-config."
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


Write-Host "Workflow" -ForegroundColor Green

# Deploy the data pipeline workflow.
Write-Host "Deploying data-pipeline workflow."
gcloud workflows deploy data-pipeline `
    --location=$REGION `
    --source=tasks/workflows/data_pipeline.yaml

Write-Host "Done. Now execute workflow manually by typing:" -ForegroundColor Green
Write-Host "  gcloud workflows run data-pipeline --location=$REGION"