-- Create or replace the core table for Philadelphia Neighborhoods.
-- This table is a simple copy of the source neighborhoods data.
-- Neighborhoods are reference/lookup data, not property-specific records.

CREATE OR REPLACE TABLE `{project_id}.core.neighborhoods`
AS (
    SELECT *
    FROM `{project_id}.source.neighborhoods`
);
