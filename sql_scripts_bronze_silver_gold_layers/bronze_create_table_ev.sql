USE ev_charge_station;

--(Issues with characters after loading. Replaced with flat file import into database)
--IF OBJECT_ID ('bronze.charging_stations_2025_world', 'U') IS NOT NULL
	--DROP TABLE bronze.charging_stations_2025_world;
--CREATE TABLE bronze.charging_stations_2025_world (
	--id INT,
	--name NVARCHAR(MAX),
	--city NVARCHAR(MAX),
	--country_code NVARCHAR(MAX),
	--state_province NVARCHAR(MAX),
	--latitude NVARCHAR(MAX),
	--longitude NVARCHAR(MAX),
	--ports NVARCHAR(MAX),
	--power_kw NVARCHAR(MAX),
	--power_class NVARCHAR(MAX),
	--is_fast_dc NVARCHAR(MAX)
--);

-- Drop table if it exists.
-- Create new table for the bronze layer to import data from csvs
IF OBJECT_ID ('bronze.country_summary_2025','U') IS NOT NULL
	DROP TABLE bronze.country_summary_2025;
CREATE TABLE bronze.country_summary_2025 (
	country_code NVARCHAR(50),
	stations INT
);

IF OBJECT_ID ('bronze.ev_models_2025', 'U') IS NOT NULL
	DROP TABLE bronze.ev_models_2025;
CREATE TABLE bronze.ev_models_2025 (
	make NVARCHAR(MAX),
	model NVARCHAR(MAX),
	market_regions NVARCHAR(MAX),
	powertrain NVARCHAR(MAX),
	first_year INT,
	body_style NVARCHAR(MAX),
	origin_country NVARCHAR(MAX)
);

IF OBJECT_ID ('bronze.world_summary_2025', 'U') IS NOT NULL
	DROP TABLE bronze.world_summary_2025;
CREATE TABLE bronze.world_summary_2025 (
	country_code NVARCHAR(50),
	country NVARCHAR(50),
	count INT,
	max_power_kw_max INT
);
