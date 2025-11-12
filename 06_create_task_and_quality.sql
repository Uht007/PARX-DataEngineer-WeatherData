-- 06_create_task_and_quality.sql
-- Create an hourly task to load new data
CREATE OR REPLACE TASK WEATHER_HOURLY_TASK
  WAREHOUSE = 'COMPUTE_WH'
  SCHEDULE = 'USING CRON 0 * * * * America/New_York'
AS
  CALL RAW.LOAD_WEATHER();

-- Enable the task
ALTER TASK WEATHER_HOURLY_TASK RESUME;

-- Simple data quality checks
-- 1. Count rows per location
SELECT LOCATION_NAME, COUNT(*) AS ROWS_COUNT
FROM STG.WEATHER_HOURLY
GROUP BY LOCATION_NAME;

-- 2. Check for missing timestamps
SELECT COUNT(*) AS MISSING_TIMES
FROM STG.WEATHER_HOURLY
WHERE TIME IS NULL;

-- 3. Check for duplicate timestamps per location
SELECT LOCATION_NAME, TIME, COUNT(*) AS DUPLICATES
FROM STG.WEATHER_HOURLY
GROUP BY LOCATION_NAME, TIME
HAVING COUNT(*) > 1;
