-- Create or replace the core table for SEPTA Stations.
-- This table is a simple copy of the source stations data.
-- Stations are reference/lookup data, not property-specific records.

CREATE OR REPLACE TABLE `{project_id}.core.septa`
AS (
    SELECT *
    FROM `{project_id}.source.septa`
);
