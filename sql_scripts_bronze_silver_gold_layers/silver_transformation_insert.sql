USE ev_charge_station;

-- Final queries for transformations of bronze layer tables 
-- Insert the final transformed bronze tables into new silver layer table for each table

-------------------------------------------------------------------------------
--Table 1: charging_stations_2025_world

WITH cleaned AS (
    SELECT
        *,
        CASE 
            WHEN power_class IS NULL 
                 OR LTRIM(RTRIM(UPPER(power_class))) IN ('', 'UNKNOWN')
                 OR power_class NOT LIKE '%[_]%[_]%' 
            THEN 'n/a'
            ELSE power_class
        END AS valid_power_class
    FROM bronze.charging_stations_2025_world
),
range_split AS (
    SELECT
        *,
        TRIM(REPLACE(REPLACE(REPLACE(
            RIGHT(
                valid_power_class,
                LEN(valid_power_class) - CHARINDEX('_', valid_power_class, CHARINDEX('_', valid_power_class) + 1)
            ), '(', ''), ')', ''), 'kW', '')) AS power_range_clean
    FROM cleaned
)

INSERT INTO silver.charging_stations_2025_world (
	id,
	name,
	city,
	country_code,
	state_province,
	latitude,
    longitude,
	ports,
    power_kw,
    type,
    speed_class,
    lower_bound_power,
    upper_bound_power,
    is_fast_dc
)

SELECT
	TRY_CAST(id AS INT) AS id,
	CASE WHEN TRIM(UPPER(name)) IN ('#NAME?', 'UNKNOWN') OR name IS NULL THEN 'n/a'
		ELSE name
	END AS name,
	COALESCE(TRIM(city), 'n/a') AS city,
	TRIM(country_code),
	COALESCE(TRIM(state_province), 'n/a') AS state_province,
	TRY_CAST(latitude AS FLOAT) AS latitude,
	TRY_CAST(longitude AS FLOAT) AS longitude,
	CASE WHEN TRY_CAST(ports AS INT) < 0 THEN 0
		ELSE TRY_CAST(ports AS INT)
	END AS ports,
	COALESCE(TRY_CAST(power_kw AS FLOAT), 0) AS power_kw,
    CASE 
        WHEN valid_power_class = 'n/a' THEN 'n/a'
        ELSE LEFT(valid_power_class, CHARINDEX('_', valid_power_class) - 1)
    END AS type,
    CASE 
        WHEN valid_power_class = 'n/a' THEN 'n/a'
        ELSE SUBSTRING(
                 valid_power_class,
                 CHARINDEX('_', valid_power_class) + 1,
                 CHARINDEX('_', valid_power_class, CHARINDEX('_', valid_power_class) + 1)
                 - CHARINDEX('_', valid_power_class) - 1
             )
    END AS speed_class,
    CASE 
        WHEN power_range_clean LIKE '>=%' THEN TRY_CAST(SUBSTRING(power_range_clean, 3, LEN(power_range_clean)) AS FLOAT)
        WHEN power_range_clean LIKE '%-%' THEN TRY_CAST(LEFT(power_range_clean, CHARINDEX('-', power_range_clean)-1) AS FLOAT)
        ELSE 0
    END AS lower_bound_power,
    CASE 
        WHEN power_range_clean LIKE '<%' THEN TRY_CAST(SUBSTRING(power_range_clean, 2, LEN(power_range_clean)) AS FLOAT)
        WHEN power_range_clean LIKE '%-%' THEN TRY_CAST(RIGHT(power_range_clean, LEN(power_range_clean) - CHARINDEX('-', power_range_clean)) AS FLOAT)
        ELSE 0
    END AS upper_bound_power,
    is_fast_dc
FROM range_split


-------------------------------------------------------------------------------------------------------
-- Table 2: country_summary_2025
-- no transformations required

INSERT INTO silver.country_summary_2025 (
    country_code,
    stations
)

SELECT
    country_code,
    stations
FROM bronze.country_summary_2025

-------------------------------------------------------------------------------------------------------
-- Table 3: ev_models_2025

INSERT INTO silver.ev_models_2025 (
    make,
    model,
    market_regions,
    powertrain,
    first_year,
    body_style,
    origin_country
)

SELECT
    make,
    model,
    LTRIM(RTRIM(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(market_regions,'(limited)', ''),
            '(select)', ''),
            '(discontinued 2023)', ''),
            '(discontinued 2023; returning)', ''),
            '(reintroduced)', ''),
            '(rolling out)', ''),
            '(2024-2025)', ''),
            '(from 2025)', ''),
            'GLOBAL', ''),
            '(', ''),
            ')', '')
    )) AS market_regions,
    powertrain,
    first_year,
    TRIM(REPLACE(body_style, '(3-row)', '')) AS body_style,
    origin_country
FROM bronze.ev_models_2025


-------------------------------------------------------------------------------------------------------
-- Table 4: world_summary_2025
INSERT INTO silver.world_summary_2025 (
    country_code,
    country,
    count,
    max_power_kw_max
)

SELECT
    country_code,
    country,
    count,
    CASE WHEN max_power_kw_max IS NULL THEN 0
        ELSE max_power_kw_max
    END AS max_power_kw_max
FROM bronze.world_summary_2025
