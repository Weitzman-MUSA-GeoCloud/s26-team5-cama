CREATE OR REPLACE TABLE `{project_id}.derived.current_assessments_model_training_data` AS

WITH bundle_sales AS (
    SELECT
        sale_price,
        DATE(sale_date) AS sale_date
    FROM `{project_id}.core.opa_properties`
    WHERE
        sale_price > 1
        AND sale_date IS NOT NULL
    GROUP BY
        sale_price,
        DATE(sale_date)
    HAVING COUNT(*) > 1
)

SELECT
    parcel_number,
    sale_price,
    DATE(sale_date) AS sale_date,
    EXTRACT(YEAR FROM sale_date) AS sale_year,
    total_area,
    total_livable_area,
    year_built,
    DATE_DIFF(CURRENT_DATE(), DATE(CAST(year_built AS STRING) || '-01-01'), YEAR)
        AS property_age,
    number_of_bedrooms,
    number_of_bathrooms,
    exterior_condition,
    interior_condition,
    category_code,
    category_code_description,
    zip_code,
    zoning,
    building_code_new,
    building_code_description_new,
    census_tract,
    geometry
FROM `{project_id}.core.opa_properties` AS p
WHERE
    sale_price > 1
    AND sale_date IS NOT NULL
    AND DATE(sale_date) >= '2015-01-01'
    AND category_code = '1'
    AND NOT EXISTS (
        SELECT 1
        FROM bundle_sales AS b
        WHERE
            b.sale_price = p.sale_price
            AND b.sale_date = DATE(p.sale_date)
    )
