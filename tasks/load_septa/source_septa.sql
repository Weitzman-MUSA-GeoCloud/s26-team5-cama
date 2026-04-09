-- Create or replace the external table for SEPTA Stations.
-- This table is backed by the Parquet file in Cloud Storage.
-- Schema is auto-detected from Parquet metadata.

CREATE OR REPLACE EXTERNAL TABLE `{project_id}.source.septa`
OPTIONS (
    format = 'PARQUET',
    uris = ['gs://{bucket_name}/septa/data.parquet']
);
