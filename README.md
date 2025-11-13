# PARX-DataEngineer-WeatherData
Weather Data Assessment for PARX Data Engineer 
Fetches historical hourly weather data from the Open-Meteo API, loads it into Snowflake, and models it for analytics.

Pipeline Flow
- Open-Meteo API → RAW → STG → DW

Setup

- Login to Snowflake Web UI.
- Select your warehouse and Database.
- Run SQL worksheets in order:
- 01_create_schemas_and_raw.sql - Run once to create Schemas                         
- 02_create_python_proc.sql - Run once to create Python API procedure                        
- 03_transform_staging.sql - Run once to create Staging procedure after API  
- 04_test_and_ingest.sql - Run on-demand
- 05_model_dw.sql - Run once to create DW procedure after Staging 
- 06_create_task_and_quality.sql - Run once to configure automation

Verify data with:
- SELECT * FROM RAW.WEATHER_JSON LIMIT 10;
- SELECT * FROM STG.WEATHER_HOURLY LIMIT 10;
- SELECT * FROM DW.FCT_WEATHER LIMIT 10;
