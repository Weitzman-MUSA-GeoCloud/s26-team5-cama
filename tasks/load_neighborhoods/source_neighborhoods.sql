-- Create or replace the external table for Philadelphia Neighborhoods.
-- This table is backed by the Parquet file in Cloud Storage.
-- Schema is auto-detected from Parquet metadata.

CREATE OR REPLACE EXTERNAL TABLE `{project_id}.source.neighborhoods`
OPTIONS (
    format = 'PARQUET',
    uris = ['gs://{bucket_name}/neighborhoods/data.parquet']
);
