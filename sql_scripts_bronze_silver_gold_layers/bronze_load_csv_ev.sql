USE ev_charge_station;
EXEC bronze.load_bronze

-- Created procedure for loading csv files into the tables created for bronze layer
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time AS DATE, @end_time AS DATE, @batch_start_time AS DATE, @batch_end_time AS DATE

	BEGIN TRY
		SET @batch_start_time = GETDATE();

		-- ORDER
		-- Truncate old table if it exists
		-- Load new data from csv
		-- Measure of time duration and error handling included

		----------------------------------------------------------------------------------------------------
		-- Table 1 (Issues with characters after loading. Replaced with flat file import into database)
		--PRINT '===> Truncating old table: bronze.charging_stations_2025_world';
		--SET @start_time = GETDATE();
		--TRUNCATE TABLE bronze.charging_stations_2025_world;
		--PRINT '===> Truncating completed';

		--PRINT '===> Loading data into table: bronze.charging_stations_2025_world';
		--BULK INSERT bronze.charging_stations_2025_world
		--FROM 'C:\Users\kevin\Documents\SQL Server Management Studio\global_ev_charging_station\dataset\charging_stations_2025_world.csv'
		--WITH (
			--FIRSTROW = 2,
			--FIELDTERMINATOR = ',',
			--TABLOCK
		--);

		--SET @end_time = GETDATE();
		--PRINT '===> Data successfully transferred: bronze.charging_stations_2025_world';
		--PRINT '===> Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		----------------------------------------------------------------------------------------------------
		-- Table 2
		PRINT '===> Truncating old table: bronze.country_summary_2025';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.country_summary_2025;
		PRINT '===> Truncating completed';

		PRINT '===> Loading data into table: bronze.country_summary_2025';
		BULK INSERT bronze.country_summary_2025
		FROM 'C:\Users\kevin\Documents\SQL Server Management Studio\global_ev_charging_station\dataset\country_summary_2025.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '===> Data successfully transferred: bronze.country_summary_2025';
		PRINT '===> Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		----------------------------------------------------------------------------------------------------
		-- Table 3
		PRINT '===> Truncating old table: bronze.ev_models_2025';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.ev_models_2025;
		PRINT '===> Truncating completed';

		PRINT '===> Loading data into table: bronze.ev_models_2025';
		BULK INSERT bronze.ev_models_2025
		FROM 'C:\Users\kevin\Documents\SQL Server Management Studio\global_ev_charging_station\dataset\ev_models_2025.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			CODEPAGE = '65001',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '===> Data successfully transferred: bronze.ev_models_2025';
		PRINT '===> Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		----------------------------------------------------------------------------------------------------
		-- Table 4
		PRINT '===> Truncating old table: bronze.world_summary_2025';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.world_summary_2025;
		PRINT '===> Truncating completed';
		
		PRINT '===> Loading data into table: bronze.world_summary_2025';
		BULK INSERT bronze.world_summary_2025
		FROM 'C:\Users\kevin\Documents\SQL Server Management Studio\global_ev_charging_station\dataset\world_summary_2025.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '===> Data successfully transferred: bronze.world_summary_2025';
		PRINT '===> Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		----------------------------------------------------------------------------------------------------
		SET @batch_end_time = GETDATE();
		PRINT '===> Loading Bronze Layer Complete';
		PRINT '===> Total Duration: ' + CAST (DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
	END TRY

	BEGIN CATCH
		PRINT 'Error occured while loading bronze layer';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Message: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message: ' + CAST(ERROR_STATE() AS NVARCHAR);
	END CATCH

END
