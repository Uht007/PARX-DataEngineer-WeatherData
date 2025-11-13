USE DATABASE DE_2;

-- 02_create_python_proc.sql
CREATE OR REPLACE PROCEDURE RAW.LOAD_WEATHER()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'requests')
HANDLER = 'main'
AS
$$
import snowflake.snowpark as snowpark
import requests
from datetime import date, timedelta, datetime
import json

def main(session: snowpark.Session):

    locations = [
        {"name": "parx_bensalem", "lat": 40.1164, "lon": -74.9613},
        {"name": "parx_shippensburg", "lat": 40.0506, "lon": -77.5205},
        {"name": "gunlake_mi", "lat": 42.6260, "lon": -85.6405},
        {"name": "greenmount_md", "lat": 39.6092, "lon": -76.8533},
    ]

    today = date.today()
    inserted_rows = 0
    skipped_rows = 0

    for loc in locations:
        query = f"""
            SELECT MAX(PAYLOAD:hourly:time[0]::date)
            FROM RAW.WEATHER_JSON
            WHERE LOCATION_NAME = '{loc['name']}'
        """
        result = session.sql(query).collect()
        last_loaded_date = result[0][0]

        if last_loaded_date is not None:
            start_date = (datetime.strptime(str(last_loaded_date), "%Y-%m-%d") + timedelta(days=1)).date()
        else:
            start_date = today - timedelta(days=1)

        if start_date >= today:
            skipped_rows += 1
            continue

        start_date_str = start_date.isoformat()
        end_date_str = today.isoformat()

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

            session.sql("""
                INSERT INTO RAW.WEATHER_JSON (LOCATION_NAME, LOAD_TS, PAYLOAD)
                SELECT ?, CURRENT_TIMESTAMP(), PARSE_JSON(?)
            """, (loc["name"], json.dumps(data))).collect()

            inserted_rows += 1

        except Exception as e:
            session.sql("""
                INSERT INTO RAW.WEATHER_JSON (LOCATION_NAME, LOAD_TS, PAYLOAD)
                SELECT ?, CURRENT_TIMESTAMP(), PARSE_JSON(?)
            """, (loc["name"], json.dumps({"error": str(e)}))).collect()

    # Call the separate transform stored procedure
    session.sql("CALL STG.TRANSFORM_WEATHER();").collect()

    return f" Loaded {inserted_rows} new location(s), skipped {skipped_rows}. Transform procedure executed."
$$;
