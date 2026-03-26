-- Create or replace the external table for OPA Properties.
-- This table is backed by the GeoParquet file in Cloud Storage.
-- Schema is auto-detected from Parquet metadata.

CREATE OR REPLACE EXTERNAL TABLE `{project_id}.source.opa_properties`
OPTIONS (
    format = 'PARQUET',
    uris = ['gs://{bucket_name}/opa_properties/data.parquet']
);
