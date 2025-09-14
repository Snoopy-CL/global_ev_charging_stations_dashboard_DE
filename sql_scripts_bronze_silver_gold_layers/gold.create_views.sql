USE ev_charge_station;

-- CREATE FACT TABLE Join country_summary_2025 and world_summary_2025 to charging_stations_2025_world
-- CREATE VIEW
-- Updated column names address the data better
-- Joined tables to create Fact table
-- Added key for reference 

CREATE VIEW gold.fact_charging_station AS
SELECT
	ROW_NUMBER() OVER (ORDER BY id) AS charging_station_key,
	ch.id AS charging_station_id,
    ch.name AS station_name,
    wo.country,
    ch.city,
    ch.state_province,
    wo.count AS number_of_stations,
    ch.latitude,
    ch.longitude,
    ch.ports,
    ch.power_kw AS recommended_power_kw,
    ch.type AS current_type,
    ch.speed_class AS charging_speed,
    ch.lower_bound_power AS charging_minimum_power_range_kw,
    ch.upper_bound_power AS charging_maximum_power_range_kw,
	wo.max_power_kw_max AS maximum_power_output_kw
FROM silver.charging_stations_2025_world ch
LEFT JOIN silver.country_summary_2025 co
	ON ch.country_code = co.country_code
LEFT JOIN silver.world_summary_2025 wo
	ON ch.country_code = wo.country_code

-- CREATE DIMENSIONAL TABLE ev_models_2025
-- CREATE VIEW
-- Updated column names address the data better
-- Joined tables to create Dimensional table
-- Edited missing data after joining tables

CREATE VIEW gold.dimensional_ev AS
SELECT
    ev.make AS brand,
    ev.model,
    CASE WHEN ev.origin_country = 'KR' THEN 'South Korea'
        WHEN ev.origin_country = 'SE/CN' THEN 'Sweden / China'
        WHEN ev.origin_country = 'RO/CN' THEN 'Romania / China'
        WHEN ev.origin_country = 'UK' THEN 'United Kingdom'
        WHEN ev.origin_country = 'CN/DE' THEN 'China / Germany'
        ELSE wo.country
    END AS origin_country,
    ev.body_style AS vehicle_type,
    ev.powertrain,
    ev.first_year AS release_year,
    ev.market_regions
FROM silver.ev_models_2025 ev
LEFT JOIN silver.world_summary_2025 wo
    ON ev.origin_country = wo.country_code