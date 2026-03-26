-- Create or replace the external table for PWD Parcels.
-- This table is backed by the GeoParquet file in Cloud Storage.
-- Schema is auto-detected from Parquet metadata.

CREATE OR REPLACE EXTERNAL TABLE `{project_id}.source.pwd_parcels`
OPTIONS (
    format = 'PARQUET',
    uris = ['gs://{bucket_name}/pwd_parcels/data.parquet']
);
