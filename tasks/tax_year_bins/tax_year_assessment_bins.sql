-- Issue #7: Create Tax Year Assessment Bins
-- Completed By: Joshua Rigsby
-- Language: SQL
--
-- Description:
-- This sql script creates a derived table that summarizes
-- property assessment values by tax year using numeric bins.
--
-- Data:
-- source.opa_assessments
--
-- Summary:
-- 1. Filter out invalid values null and 0 market values
-- 2. Group properties into value bins:
--    - 0 to 100,000
--    - 100,000 to 200,000
--    - 200,000 to 500,000
--    - 500,000 to 1,000,000
--    - 1,000,000+
-- 3. Count number of properties in each bin per year


CREATE OR REPLACE TABLE `musa5090s26-team5.derived.tax_year_assessment_bins` AS
SELECT
    CAST(year AS INT64) AS tax_year,

    CASE
        WHEN market_value < 100000 THEN 0
        WHEN market_value < 200000 THEN 100000
        WHEN market_value < 500000 THEN 200000
        WHEN market_value < 1000000 THEN 500000
        ELSE 1000000
    END AS lower_bound,

    CASE
        WHEN market_value < 100000 THEN 100000
        WHEN market_value < 200000 THEN 200000
        WHEN market_value < 500000 THEN 500000
        WHEN market_value < 1000000 THEN 1000000
        ELSE 999999999
    END AS upper_bound,

    COUNT(*) AS property_count

FROM `musa5090s26-team5.source.opa_assessments`

WHERE
    market_value IS NOT NULL
    AND market_value > 0

GROUP BY tax_year, lower_bound, upper_bound
ORDER BY tax_year, lower_bound;
