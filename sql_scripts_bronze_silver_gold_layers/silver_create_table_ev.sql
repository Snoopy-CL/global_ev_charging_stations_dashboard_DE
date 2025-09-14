USE ev_charge_station;

-- Drop table if old silver layer table exists
-- Create a new silver layer table with updated data types from the bronze layer
-- Added date for when table was created
IF OBJECT_ID ('silver.charging_stations_2025_world', 'U') IS NOT NULL
	DROP TABLE silver.charging_stations_2025_world;
CREATE TABLE silver.charging_stations_2025_world (
	id INT,
    name NVARCHAR(MAX),
    city NVARCHAR(MAX),
    country_code NVARCHAR(MAX),
    state_province NVARCHAR(MAX),
    latitude FLOAT,
    longitude FLOAT,
    ports INT,
    power_kw INT,
    type VARCHAR(50),
    speed_class VARCHAR(50),
    lower_bound_power FLOAT,
    upper_bound_power FLOAT,
    is_fast_dc NVARCHAR(MAX),
    table_created_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID ('silver.country_summary_2025','U') IS NOT NULL
	DROP TABLE silver.country_summary_2025;
CREATE TABLE silver.country_summary_2025 (
	country_code NVARCHAR(50),
	stations INT,
	table_created_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID ('silver.ev_models_2025', 'U') IS NOT NULL
	DROP TABLE silver.ev_models_2025;
CREATE TABLE silver.ev_models_2025 (
	make NVARCHAR(100),
	model NVARCHAR(100),
	market_regions NVARCHAR(MAX),
	powertrain NVARCHAR(50),
	first_year INT,
	body_style NVARCHAR(50),
	origin_country NVARCHAR(50),
	table_created_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID ('silver.world_summary_2025', 'U') IS NOT NULL
	DROP TABLE silver.world_summary_2025;
CREATE TABLE silver.world_summary_2025 (
	country_code NVARCHAR(50),
	country NVARCHAR(50),
	count INT,
	max_power_kw_max INT,
	table_created_date DATETIME2 DEFAULT GETDATE()
);