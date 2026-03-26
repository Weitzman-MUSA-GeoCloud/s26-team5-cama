-- Create or replace the core table for OPA Properties.
-- This table includes all fields from the source table plus a property_id field.

CREATE OR REPLACE TABLE `{project_id}.core.opa_properties`
AS (
    SELECT
        parcel_number AS property_id,
        *
    FROM `{project_id}.source.opa_properties`
);
