USE DATABASE DE_2;

-- 03_create_python_proc.sql
CREATE OR REPLACE PROCEDURE RAW.LOAD_WEATHER()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'requests')
EXTERNAL_ACCESS_INTEGRATIONS = (open_meteo_eai)
HANDLER = 'main'
AS
$$
import snowflake.snowpark as snowpark
import requests
from datetime import datetime, timedelta, date
import json

def main(session: snowpark.Session):

    locations = [
        {"name": "parx_bensalem", "lat": 40.1164, "lon": -74.9613},
        {"name": "parx_shippensburg", "lat": 40.0506, "lon": -77.5205},
        {"name": "gunlake_mi", "lat": 42.6260, "lon": -85.6405},
        {"name": "greenmount_md", "lat": 39.6092, "lon": -76.8533},
    ]

    inserted = 0
    skipped = 0

    today = date.today()
    batch_load_ts = datetime.now()

    for loc in locations:

        # GET LAST LOADED HOUR
        query = f"""
            SELECT MAX(TO_TIMESTAMP_NTZ(f.value::string)) AS last_hour
            FROM RAW.WEATHER_JSON r,
                 LATERAL FLATTEN(input => r.PAYLOAD:hourly:time) f
            WHERE LOCATION_NAME = '{loc['name']}'
        """

        result = session.sql(query).collect()
        last_loaded_hour = result[0][0]


        # DETERMINE START DATE BASED LAST HOUR LOADED
        if last_loaded_hour is None:
            # If no data, load 2 days back
            start_date = today - timedelta(days=2)
        else:
            # Next missing hour
            next_hour = last_loaded_hour + timedelta(hours=1)
            start_date = next_hour.date()

        # If start date is today or later, nothing new available yet
        if start_date >= today:
            skipped += 1
            continue

        start_date_str = start_date.isoformat()
        end_date_str = today.isoformat()


        # CALL OPEN-METEO API
        url = (
            f"https://archive-api.open-meteo.com/v1/archive?"
            f"latitude={loc['lat']}&longitude={loc['lon']}"
            f"&start_date={start_date_str}&end_date={end_date_str}"
            "&hourly=temperature_2m,precipitation"
            "&timezone=America/New_York"
        )

        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            # INSERT NEW JSON PAYLOAD ROW
            session.sql("""
                INSERT INTO RAW.WEATHER_JSON (LOCATION_NAME, LOAD_TS, PAYLOAD)
                SELECT ?, ?, PARSE_JSON(?)
            """, (loc["name"], batch_load_ts, json.dumps(data))).collect()

            inserted += 1

        except Exception as e:
            # Insert an error record to inspect later
            session.sql("""
                INSERT INTO RAW.WEATHER_JSON (LOCATION_NAME, LOAD_TS, PAYLOAD)
                SELECT ?, ?, PARSE_JSON(?)
            """, (loc["name"], batch_load_ts, json.dumps({"error": str(e)}))).collect()
            skipped += 1

    # 5. Run STG transform AFTER inserting
    session.sql("CALL STG.TRANSFORM_WEATHER();").collect()

    return f"Inserted {inserted} locations, skipped {skipped}, batch_load_ts={batch_load_ts}"
$$;
