USE DATABASE DE_2;
USE SCHEMA STG;

-- 04_transform_staging.sql
CREATE OR REPLACE PROCEDURE STG.TRANSFORM_WEATHER()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    latest_load_ts TIMESTAMP;
BEGIN
    -- Get the most recent load timestamp
    SELECT MAX(LOAD_TS) INTO latest_load_ts
    FROM RAW.WEATHER_JSON;

    -- Flatten the most recent load into a temp table using dynamic SQL
    EXECUTE IMMEDIATE '
        CREATE OR REPLACE TEMPORARY TABLE STG.WEATHER_HOURLY_TMP AS
        SELECT
            LOCATION_NAME,
            TO_TIMESTAMP_NTZ(time.value::string) AS TIME,
            temperature.value::FLOAT AS TEMPERATURE,
            precipitation.value::FLOAT AS PRECIPITATION,
            LOAD_TS
        FROM RAW.WEATHER_JSON r,
             LATERAL FLATTEN(input => r.PAYLOAD:hourly:time) time,
             LATERAL FLATTEN(input => r.PAYLOAD:hourly:temperature_2m) temperature,
             LATERAL FLATTEN(input => r.PAYLOAD:hourly:precipitation) precipitation
        WHERE time.index = temperature.index
          AND temperature.index = precipitation.index
          AND r.LOAD_TS = ''' || latest_load_ts || '''';
    
    -- Merge incremental results
    MERGE INTO STG.WEATHER_HOURLY t
    USING STG.WEATHER_HOURLY_TMP s
    ON t.LOCATION_NAME = s.LOCATION_NAME
       AND t.TIME = s.TIME
    WHEN MATCHED THEN UPDATE SET
        t.TEMPERATURE = s.TEMPERATURE,
        t.PRECIPITATION = s.PRECIPITATION,
        t.LOAD_TS = s.LOAD_TS
    WHEN NOT MATCHED THEN
        INSERT (LOCATION_NAME, TIME, TEMPERATURE, PRECIPITATION, LOAD_TS)
        VALUES (s.LOCATION_NAME, s.TIME, s.TEMPERATURE, s.PRECIPITATION, s.LOAD_TS);

    RETURN 'Transform complete for load timestamp: ' || latest_load_ts;
END;
$$;
