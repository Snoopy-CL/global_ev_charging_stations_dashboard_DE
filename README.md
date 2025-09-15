# global_ev_charging_stations_dashboard_DE

## Project Overview
This project explores global electric vehicle (EV) charging infrastructure using a structured data engineering pipeline and an interactive dashboard.

Raw data from Kaggle’s Global EV Charging Stations dataset is processed through a Bronze–Silver–Gold ETL architecture in SQL Server. The cleaned and transformed data is then visualized in Power BI, highlighting insights into the growth and distribution of EV charging stations worldwide.

The project transforms raw, large-scale EV charging data into clean, analytics-ready datasets, enabling insights into infrastructure distribution and trends.

## Tech Stack
**SQL Server**: ETL pipeline, data transformation, schema modeling.\
**Power BI**: Interactive dashboards with maps, tables, and Q & A functionality.

## ETL Architecture
The ETL pipeline is organized using the Bronze–Silver–Gold framework:
**Bronze Layer**: 
- Ingests raw, unprocessed data directly from Kaggle csv. 
- Stores the data in its original format for traceability.

**Silver Layer**
- Cleans and transforms the raw data, handling nulls, duplicates, and formatting inconsistencies.
- Splits dense columns into separate fields and converts abbreviations to full descriptive terms to improve clarity.

**Gold Layer**
- Finalizes cleaned data with descriptive column names.
- Implements table joins, dimensional models, and fact views for analytics.
- Serves as the source for the Power BI dashboard, optimized for reporting and insights.

Engineering Features:
- Error handling and logging for maintainability.
- Timers and progress statements to monitor performance.
- Designed for debuggable, production-ready workflows.

## Dashboard
The Power BI dashboard visualizes processed data, enabling users to explore:
- Interactive global map showing EV charging station locations.
- Tables summarizing vehicle brand information.
- Q & A functionality for dynamic exploration of the data.
- KPI cards displaying key metrics.

## Summary of Files
"dataset_kaggle" - Contains all raw datasets.\
"gold_layer_output_views" - Contains final outputs for both dimensional and fact views.\
"sql_scripts_bronze_silver_gold_layers" - Contains all SQL Server scripts made for bronze, silver, and gold layers.\
"ev_charging_stations.pbix" - Power BI file for dashboard.\
"ev_charging_stations_dashboard_image.pdf" - PDF of dashboard. Quick image to get a view of the dashboard.
