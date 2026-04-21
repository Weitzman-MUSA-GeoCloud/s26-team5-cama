-- Issue #8: Develop a Table for Current Assessment Value Distributions
-- Completed By: Joshua Rigsby
-- Language: SQL (obvi)
-- 
-- Description:
-- Script creates a derived table that summarizes
-- current predicted property assessment values using numeric bins.
-- 
-- Used:
-- derived.current_assessments
-- 
-- Summary:
-- 1. Filter out invalid null and 0 predicted values
-- 2. Group properties into equal-width linear value bins - $100,000 each:
--    - 0 to 100,000
--    - 100,000 to 200,000
--    - 200,000 to 300,000
--    - 300,000 to 400,000
--    - 400,000 to 500,000
--    - 500,000+
-- 3. Count number of properties in each bin


CREATE OR REPLACE TABLE `musa5090s26-team5.derived.current_assessment_bins` AS
SELECT
    CASE
        WHEN predicted_value < 100000 THEN 0
        WHEN predicted_value < 200000 THEN 100000
        WHEN predicted_value < 300000 THEN 200000
        WHEN predicted_value < 400000 THEN 300000
        WHEN predicted_value < 500000 THEN 400000
        ELSE 500000
    END AS lower_bound,

    CASE
        WHEN predicted_value < 100000 THEN 100000
        WHEN predicted_value < 200000 THEN 200000
        WHEN predicted_value < 300000 THEN 300000
        WHEN predicted_value < 400000 THEN 400000
        WHEN predicted_value < 500000 THEN 500000
        ELSE 999999999
    END AS upper_bound,

    COUNT(*) AS property_count

FROM `musa5090s26-team5.derived.current_assessments`

WHERE
    predicted_value IS NOT NULL
    AND predicted_value > 0

GROUP BY lower_bound, upper_bound
ORDER BY lower_bound;
