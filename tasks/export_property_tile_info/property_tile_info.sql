-- Issue #27: Export GeoJSON File with Assessment Values for Residential Properties
-- Completed By: Joshua Rigsby
-- Language: SQL (obvi)
-- 
-- Description:
-- Script queries residential property data for export as a
-- GeoJSON file to be converted into vector tiles for the assessment dashboard.
-- 
-- Used:
-- core.pwd_parcels
-- core.opa_properties
-- core.opa_assessments
-- core.neighborhoods
-- derived.current_assessments
-- 
-- Summary:
-- 1. Query the most recent official tax year assessment value per property
-- 2. Transform to get assessment history to get 2023 and 2024 assessed values
-- 3. Join parcel geometry (pwd_parcels) with property characteristics
--    (opa_properties), official assessments (opa_assessments), and
--    ML-predicted values (derived.current_assessments)
-- 4. Calculate percent change between predicted and last official value
-- 5. Filter to residential properties only
-- 6. Output geometry and selected fields for hover tooltip and
--    click-popup panel in dashboard



WITH latest_assessment AS (
    SELECT
        parcel_number,
        market_value AS tax_year_assessed_value,
        year AS tax_year
    FROM (
        SELECT
            parcel_number,
            market_value,
            year,
            ROW_NUMBER()
                OVER (PARTITION BY parcel_number ORDER BY year DESC)
                AS rn
        FROM `musa5090s26-team5.core.opa_assessments`
        WHERE
            market_value IS NOT NULL
            AND market_value > 0
    )
    WHERE rn = 1
),

assessments_pivot AS (
    SELECT
        parcel_number,
        MAX(CASE WHEN year = 2023.0 THEN market_value END)
            AS assessed_value_2023,
        MAX(CASE WHEN year = 2024.0 THEN market_value END)
            AS assessed_value_2024
    FROM `musa5090s26-team5.core.opa_assessments`
    WHERE
        market_value IS NOT NULL
        AND market_value > 0
    GROUP BY parcel_number
)

SELECT
    -- Geometry
    ST_ASGEOJSON(pwd.geometry) AS geometry,

    -- Id
    p.parcel_number AS property_id,

    -- Hover tooltip fields
    p.location AS address,
    ca.predicted_value AS current_assessed_value,
    -- 80% prediction interval from quantile models (explainability/MOE).
    ca.predicted_value_lower AS current_assessed_value_lower,
    ca.predicted_value_upper AS current_assessed_value_upper,
    la.tax_year_assessed_value,
    la.tax_year,
    ROUND(
        SAFE_DIVIDE(
            ca.predicted_value - la.tax_year_assessed_value,
            la.tax_year_assessed_value
        ),
        4
    ) AS pct_change,
    p.zip_code,

    -- Click-popup panel fields
    ap.assessed_value_2023,
    ap.assessed_value_2024,
    SAFE_CAST(p.total_livable_area AS FLOAT64) AS total_livable_area,
    SAFE_CAST(p.number_of_bathrooms AS FLOAT64) AS number_of_bathrooms,
    SAFE_CAST(p.interior_condition AS FLOAT64) AS interior_condition,
    SAFE_CAST(p.year_built AS INT64) AS year_built,
    EXTRACT(YEAR FROM SAFE_CAST(p.sale_date AS DATE)) AS sale_year,
    n.name AS neighborhood

FROM `musa5090s26-team5.core.opa_properties` AS p
INNER JOIN `musa5090s26-team5.core.pwd_parcels` AS pwd
    ON pwd.property_id = p.parcel_number
LEFT JOIN latest_assessment AS la
    ON p.parcel_number = CAST(la.parcel_number AS STRING)
LEFT JOIN `musa5090s26-team5.derived.current_assessments` AS ca
    ON p.parcel_number = ca.property_id
LEFT JOIN assessments_pivot AS ap
    ON p.parcel_number = CAST(ap.parcel_number AS STRING)
LEFT JOIN `musa5090s26-team5.core.neighborhoods` AS n
    ON ST_WITHIN(
        p.geometry,
        ST_GEOGFROMWKB(n.geometry)
    )
WHERE
    p.category_code = '1'
    AND pwd.geometry IS NOT NULL
