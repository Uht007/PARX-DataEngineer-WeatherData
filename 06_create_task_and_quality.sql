USE DATABASE DE_2;

-- 06_create_task_and_quality.sql
-- Create an hourly task to load new data and update DW
CREATE OR REPLACE TASK WEATHER_HOURLY_TASK
  WAREHOUSE = 'COMPUTE_WH'
  SCHEDULE = 'USING CRON 0 * * * * America/New_York'
AS
BEGIN
  -- Load new weather data into RAW + transform to STG
  CALL RAW.LOAD_WEATHER();

  -- Incrementally update DW fact table
  CALL DW.TRANSFORM_FCT_WEATHER();
END;

-- Enable the task
ALTER TASK WEATHER_HOURLY_TASK RESUME;

-- Count rows per location
SELECT LOCATION_NAME, COUNT(*) AS ROWS_COUNT
FROM STG.WEATHER_HOURLY
GROUP BY LOCATION_NAME;

-- Check for missing timestamps
SELECT COUNT(*) AS MISSING_TIMES
FROM STG.WEATHER_HOURLY
WHERE TIME IS NULL;

-- Check for duplicate timestamps per location
SELECT LOCATION_NAME, TIME, COUNT(*) AS DUPLICATES
FROM STG.WEATHER_HOURLY
GROUP BY LOCATION_NAME, TIME
HAVING COUNT(*) > 1;
