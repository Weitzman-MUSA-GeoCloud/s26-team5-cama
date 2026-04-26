-- Issue #12: Inference query for predict_assessments Cloud Function.
-- Language: SQL
--
-- Description:
-- Reads the raw features needed to score the trained valuation model
-- (eda/model.pkl) on every residential property in the city.
--
-- Used:
-- core.opa_properties
-- core.opa_assessments
-- core.neighborhoods
--
-- Notes:
-- 1. Mirrors the feature derivation in
--    `tasks/create_training_data/create_training_data.sql`, but for ALL
--    residential properties (no sale-date / bundle filtering).
-- 2. Most fields are returned raw; the prediction Cloud Function imputes
--    missing values from the training-time medians stored in `model.pkl`
--    so imputation logic stays in one place (Python, not split across
--    SQL + Python with potentially different fill values).
-- 3. `sale_year` is intentionally set to the current year. The model uses
--    `sale_year` as a recency signal -- for "what is this property worth
--    NOW?" the right value is the current year, regardless of when (or
--    whether) the property last sold.
-- 4. `neighborhood_median_price` is NOT derived in SQL; it is looked up
--    from `bundle["neighborhood_price_map"]` in `main.py`.

WITH assessments_pivot AS (
    SELECT
        parcel_number,
        MAX(CASE WHEN year = 2023.0 THEN market_value END) AS assessed_value_2023,
        MAX(CASE WHEN year = 2024.0 THEN market_value END) AS assessed_value_2024,
        MAX(CASE WHEN year = 2025.0 THEN market_value END) AS assessed_value_2025
    FROM `{project_id}.core.opa_assessments`
    WHERE
        market_value IS NOT NULL
        AND market_value > 0
    GROUP BY parcel_number
),

prop_neighborhood AS (
    SELECT
        p.parcel_number,
        n.name AS neighborhood
    FROM `{project_id}.core.opa_properties` AS p
    INNER JOIN `{project_id}.core.neighborhoods` AS n
        ON ST_CONTAINS(ST_GEOGFROMWKB(n.geometry), p.geometry)
    WHERE
        p.category_code = '1'
        AND p.geometry IS NOT NULL
)

SELECT
    p.parcel_number AS property_id,
    SAFE_CAST(p.total_livable_area AS FLOAT64) AS total_livable_area,
    SAFE_CAST(p.number_of_bathrooms AS FLOAT64) AS number_of_bathrooms,
    EXTRACT(YEAR FROM CURRENT_DATE()) - SAFE_CAST(p.year_built AS INT64) AS property_age,
    SAFE_CAST(p.interior_condition AS FLOAT64) AS interior_condition,
    a.assessed_value_2023,
    a.assessed_value_2024,
    a.assessed_value_2025,
    pn.neighborhood,
    EXTRACT(YEAR FROM CURRENT_DATE()) AS sale_year
FROM `{project_id}.core.opa_properties` AS p
LEFT JOIN assessments_pivot AS a
    ON CAST(a.parcel_number AS STRING) = p.parcel_number
LEFT JOIN prop_neighborhood AS pn
    ON pn.parcel_number = p.parcel_number
WHERE p.category_code = '1';
