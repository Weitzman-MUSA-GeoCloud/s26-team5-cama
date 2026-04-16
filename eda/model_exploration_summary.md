# Model Exploration Summary

## Data Sources Explored

### 1. OPA Assessments (`core.opa_assessments`)
- 6,890,147 rows of historical assessment records
- Join key: `parcel_number`
- Recommended features:
  - `assessed_value_2023`: market value in 2023
  - `assessed_value_2024`: market value in 2024
  - `assessed_value_2025`: market value in 2025
  - `pct_change_2023_to_2025`: percent change over 2 years (signals appreciation trend)

### 2. PWD Parcels (`core.pwd_parcels`)
- 545,847 parcels with geometry
- Join key: `brt_id` = `parcel_number`
- Recommended features:
  - `lot_area_sqft`: parcel area from geometry (cross-validates OPA total_area)
  - `lot_perimeter`: perimeter of parcel polygon
  - `lot_shape_ratio`: area / perimeter^2 (compactness of lot shape)

### 3. Philadelphia Neighborhoods (OpenDataPhilly)
- 57 neighborhood polygons
- Spatial join to OPA properties via point-in-polygon
- Only 4 out of 583,365 properties had no neighborhood match
- Recommended feature:
  - `neighborhood`: categorical, stronger location signal than zip_code
  - Reduces location categories from ~100 zip codes to 57 neighborhoods

### 4. SEPTA Rail Stations
- 13 major rail/subway stations (MFL, BSL, Regional Rail)
- Recommended features:
  - `dist_to_septa_miles`: continuous distance to nearest station
  - `septa_proximity`: binned category (<0.25mi, 0.25-0.5mi, 0.5-1mi, >1mi)
- Distribution: 81% of properties are >1 mile from a station

## Confirmed Feature List for Model (Issue #11)

| Feature | Source Table | Join Key | Notes |
|---------|-------------|----------|-------|
| `assessed_value_2023` | `core.opa_assessments` | `parcel_number` | Historical value |
| `assessed_value_2024` | `core.opa_assessments` | `parcel_number` | Historical value |
| `assessed_value_2025` | `core.opa_assessments` | `parcel_number` | Most recent assessment |
| `pct_change_2023_to_2025` | `core.opa_assessments` | `parcel_number` | Appreciation signal |
| `lot_area_sqft` | `core.pwd_parcels` | `brt_id` = `parcel_number` | Geometry-derived area |
| `lot_shape_ratio` | `core.pwd_parcels` | `brt_id` = `parcel_number` | Lot compactness |
| `neighborhood` | OpenDataPhilly neighborhoods | spatial join | Categorical location feature |
| `dist_to_septa_miles` | SEPTA stations | spatial join | Transit proximity |
| `septa_proximity` | SEPTA stations | spatial join | Binned transit category |

## Features NOT Recommended
- `zip_code`: collinear with neighborhood, less granular
- Crime data: high null rates, potential bias
- Flood zones: affects <2% of Philadelphia properties
- School catchments: collinear with neighborhood

## Next Steps (Issue #11)
- Use `core.opa_properties` as base table (target: `sale_price`)
- Filter to residential properties (`category_code = 1`)
- Filter to sales since 2015 (`sale_date >= 2015-01-01`)
- Exclude dollar-sales (`sale_price > 1`)
- Join features from the tables above using `parcel_number`
