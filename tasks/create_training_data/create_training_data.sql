CREATE OR REPLACE TABLE `{project_id}.derived.current_assessments_model_training_data` AS

WITH bundle_sales AS (
    SELECT
        SAFE_CAST(sale_price AS FLOAT64) AS sale_price,
        DATE(SAFE_CAST(sale_date AS TIMESTAMP)) AS sale_date
    FROM `{project_id}.core.opa_properties`
    WHERE
        SAFE_CAST(sale_price AS FLOAT64) > 1
        AND sale_date IS NOT NULL
    GROUP BY
        SAFE_CAST(sale_price AS FLOAT64),
        DATE(SAFE_CAST(sale_date AS TIMESTAMP))
    HAVING COUNT(DISTINCT parcel_number) > 1
),

medians AS (
    SELECT
        PERCENTILE_CONT(SAFE_CAST(total_livable_area AS FLOAT64), 0.5) OVER () AS median_total_livable_area,
        PERCENTILE_CONT(SAFE_CAST(year_built AS FLOAT64), 0.5) OVER () AS median_year_built,
        PERCENTILE_CONT(SAFE_CAST(number_of_bedrooms AS FLOAT64), 0.5) OVER () AS median_bedrooms,
        PERCENTILE_CONT(SAFE_CAST(number_of_bathrooms AS FLOAT64), 0.5) OVER () AS median_bathrooms
    FROM `{project_id}.core.opa_properties`
    WHERE category_code = '1'
    LIMIT 1
),

mode_conditions AS (
    SELECT
        APPROX_TOP_COUNT(SAFE_CAST(exterior_condition AS FLOAT64), 1)[OFFSET(0)].value AS mode_exterior,
        APPROX_TOP_COUNT(SAFE_CAST(interior_condition AS FLOAT64), 1)[OFFSET(0)].value AS mode_interior
    FROM `{project_id}.core.opa_properties`
    WHERE
        category_code = '1'
        AND exterior_condition IS NOT NULL
        AND interior_condition IS NOT NULL
),

assessments_pivot AS (
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

pwd AS (
    SELECT
        brt_id,
        shape__area AS lot_area_sqft,
        shape__length AS lot_perimeter,
        SAFE_DIVIDE(shape__area, shape__length * shape__length) AS lot_shape_ratio
    FROM `{project_id}.core.pwd_parcels`
    WHERE
        brt_id IS NOT NULL
        AND shape__area > 0
),

septa_dist AS (
    SELECT
        p.parcel_number,
        MIN(ST_DISTANCE(
            p.geometry,
            ST_GEOGPOINT(SAFE_CAST(s.longitude AS FLOAT64), SAFE_CAST(s.latitude AS FLOAT64))
        )) / 1609.34 AS dist_to_septa_miles
    FROM `{project_id}.core.opa_properties` AS p
    CROSS JOIN `{project_id}.core.septa` AS s
    WHERE
        p.category_code = '1'
        AND s.longitude IS NOT NULL
        AND s.latitude IS NOT NULL
        AND p.geometry IS NOT NULL
    GROUP BY p.parcel_number
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
    p.parcel_number,
    SAFE_CAST(p.sale_price AS FLOAT64) AS sale_price,
    DATE(SAFE_CAST(p.sale_date AS TIMESTAMP)) AS sale_date,
    EXTRACT(YEAR FROM SAFE_CAST(p.sale_date AS TIMESTAMP)) AS sale_year,
    COALESCE(SAFE_CAST(p.total_livable_area AS FLOAT64), m.median_total_livable_area) AS total_livable_area,
    SAFE_CAST(p.total_area AS FLOAT64) AS total_area,
    COALESCE(SAFE_CAST(p.year_built AS FLOAT64), m.median_year_built) AS year_built,
    EXTRACT(YEAR FROM CURRENT_DATE()) - CAST(COALESCE(SAFE_CAST(p.year_built AS FLOAT64), m.median_year_built) AS INT64) AS property_age,
    COALESCE(SAFE_CAST(p.exterior_condition AS FLOAT64), mc.mode_exterior) AS exterior_condition,
    COALESCE(SAFE_CAST(p.interior_condition AS FLOAT64), mc.mode_interior) AS interior_condition,
    (
        COALESCE(SAFE_CAST(p.exterior_condition AS FLOAT64), mc.mode_exterior)
        + COALESCE(SAFE_CAST(p.interior_condition AS FLOAT64), mc.mode_interior)
    ) / 2.0 AS condition_score,
    COALESCE(SAFE_CAST(p.number_of_bedrooms AS FLOAT64), m.median_bedrooms) AS number_of_bedrooms,
    COALESCE(SAFE_CAST(p.number_of_bathrooms AS FLOAT64), m.median_bathrooms) AS number_of_bathrooms,
    p.zoning,
    a.assessed_value_2023,
    a.assessed_value_2024,
    a.assessed_value_2025,
    ROUND(
        SAFE_DIVIDE(a.assessed_value_2025 - a.assessed_value_2023, a.assessed_value_2023) * 100,
        2
    ) AS pct_change_2023_to_2025,
    pwd.lot_area_sqft,
    pwd.lot_perimeter,
    pwd.lot_shape_ratio,
    sd.dist_to_septa_miles,
    pn.neighborhood
FROM `{project_id}.core.opa_properties` AS p
CROSS JOIN medians AS m
CROSS JOIN mode_conditions AS mc
LEFT JOIN assessments_pivot AS a
    ON CAST(a.parcel_number AS STRING) = p.parcel_number
LEFT JOIN pwd
    ON CAST(pwd.brt_id AS STRING) = p.parcel_number
LEFT JOIN septa_dist AS sd
    ON sd.parcel_number = p.parcel_number
LEFT JOIN prop_neighborhood AS pn
    ON pn.parcel_number = p.parcel_number
WHERE
    SAFE_CAST(p.sale_price AS FLOAT64) > 1
    AND p.sale_date IS NOT NULL
    AND DATE(SAFE_CAST(p.sale_date AS TIMESTAMP)) >= '2015-01-01'
    AND p.category_code = '1'
    AND NOT EXISTS (
        SELECT 1
        FROM bundle_sales AS b
        WHERE
            b.sale_price = SAFE_CAST(p.sale_price AS FLOAT64)
            AND b.sale_date = DATE(SAFE_CAST(p.sale_date AS TIMESTAMP))
    )
