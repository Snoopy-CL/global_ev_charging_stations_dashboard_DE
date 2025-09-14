USE ev_charge_station;

-- Script for rough draft of queries used to make required transformations

-- QUERIES FOR charging_stations_2025_world 

-- check duplicate rows
SELECT *
FROM bronze.charging_stations_2025_world
GROUP BY id, name, state_province, city, country_code, state_province, latitude, longitude, ports, power_kw, power_class, is_fast_dc
HAVING COUNT(*) > 1;


-- check for null values
SELECT state_province
FROM bronze.charging_stations_2025_world
WHERE state_province IS NULL
	OR state_province = ''
	OR state_province IN ('#NAME?', 'N/A', 'UNKNOWN', 'NULL')


-- cast test
SELECT 
	TRY_CAST(id AS INT) AS id
FROM bronze.charging_stations_2025_world
--WHERE id IS NULL
WHERE TRY_CAST(id AS INT) IS NULL


--longitude and latitude ranges
SELECT
	longitude,
	latitude
FROM bronze.charging_stations_2025_world
--WHERE TRY_CAST(longitude AS FLOAT) NOT BETWEEN -180 and 180 OR TRY_CAST(latitude AS FLOAT) NOT BETWEEN -90 and 90
WHERE TRY_CAST(longitude AS FLOAT) IS NULL OR TRY_CAST(latitude AS FLOAT) IS NULL


-- check ports column
SELECT 
	ports
FROM bronze.charging_stations_2025_world
--WHERE ports LIKE '%.%'
--WHERE ports < 0
WHERE TRY_CAST(ports AS INT) IS NULL


--check power_kw column
SELECT 
	power_kw
FROM bronze.charging_stations_2025_world
--WHERE TRY_CAST(power_kw AS FLOAT) IS NULL -- [YES]
--WHERE power_kw IS NULL -- [YES]
WHERE TRY_CAST(power_kw AS FLOAT) < 0 


--check power_class
SELECT 
	power_class
FROM bronze.charging_stations_2025_world
--WHERE power_class IS NULL
	--OR power_class = ''
	--OR power_class IN ('%NAME?','N/A', 'UNKNOWN', 'NULL') -- Unknown
WHERE power_class NOT LIKE '%_%_%' 

--split power_class into 3 columns
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
SELECT
    power_class,
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
    power_range_clean,
    CASE 
        WHEN power_range_clean LIKE '>=%' THEN TRY_CAST(SUBSTRING(power_range_clean, 3, LEN(power_range_clean)) AS FLOAT)
        WHEN power_range_clean LIKE '%-%' THEN TRY_CAST(LEFT(power_range_clean, CHARINDEX('-', power_range_clean)-1) AS FLOAT)
        ELSE NULL
    END AS lower_bound_power,
    CASE 
        WHEN power_range_clean LIKE '<%' THEN TRY_CAST(SUBSTRING(power_range_clean, 2, LEN(power_range_clean)) AS FLOAT)
        WHEN power_range_clean LIKE '%-%' THEN TRY_CAST(RIGHT(power_range_clean, LEN(power_range_clean) - CHARINDEX('-', power_range_clean)) AS FLOAT)
        ELSE NULL
    END AS upper_bound_power
FROM range_split;


-- check is_fast_dc
SELECT DISTINCT
    is_fast_dc
FROM bronze.charging_stations_2025_world

-----------------------------------------------------------------------------------------------------------
-- QUERIES FOR country_summary_2025

-- Check country_code column
SELECT
    country_code
FROM bronze.country_summary_2025
WHERE LEN(country_code) != 2 OR country_code NOT LIKE '[A-Z][A-Z]'

-- Check stations column
SELECT
    stations
FROM bronze.country_summary_2025
WHERE stations < 0 OR stations IS NULL

-----------------------------------------------------------------------------------------------------------
-- QUERIES FOR ev_models_2025

-- Check market_regions column
SELECT DISTINCT
    market_regions
FROM bronze.ev_models_2025 -- remove brackets and extra strings

--Check body_style column
SELECT DISTINCT
    body_style
FROM bronze.ev_models_2025 -- remove (3 row)

-----------------------------------------------------------------------------------------------------------
-- QUERIES world_summary_2025

-- Check country_code column
SELECT
    country_code
FROM bronze.world_summary_2025
WHERE LEN(country_code) != 2 OR country_code NOT LIKE '[A-Z][A-Z]'

SELECT
    country_code,
    COUNT(*)
FROM bronze.world_summary_2025
GROUP BY country_code
HAVING COUNT(*) > 1 

-- Check country column
SELECT 
    country
FROM bronze.world_summary_2025
WHERE country IS NULL 
    OR country = ''

SELECT 
    country,
    COUNT(*) AS duplicate
FROM bronze.world_summary_2025
GROUP BY country
HAVING COUNT(*) > 1 

-- Check count column
SELECT
    count
FROM bronze.world_summary_2025
WHERE count < 0 OR count IS NULL 

-- Check max_power_kw_max
SELECT
    max_power_kw_max
FROM bronze.world_summary_2025
WHERE max_power_kw_max < 0 OR max_power_kw_max IS NULL -- NULL values
