-- Create or replace the core table for PWD Parcels.
-- This table includes all fields from the source table plus a property_id field.
-- Note: PWD Parcels use brt_id as the parcel identifier (BRT = Board of Revision of Taxes).

CREATE OR REPLACE TABLE `{project_id}.core.pwd_parcels`
AS (
    SELECT
        CAST(brt_id AS STRING) AS property_id,
        *
    FROM `{project_id}.source.pwd_parcels`
);
