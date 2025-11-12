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
from datetime import date, timedelta
import json

def main(session: snowpark.Session):

    locations = [
        {"name": "parx_bensalem", "lat": 40.1164, "lon": -74.9613},
        {"name": "parx_shippensburg", "lat": 40.0506, "lon": -77.5205},
        {"name": "gunlake_mi", "lat": 42.6260, "lon": -85.6405},
        {"name": "greenmount_md", "lat": 39.6092, "lon": -76.8533},
    ]

    today = date.today()
    yesterday = today - timedelta(days=1)
    start_date = yesterday.isoformat()
    end_date = today.isoformat()

    inserted_rows = 0

    for loc in locations:
        url = (
            f"https://archive-api.open-meteo.com/v1/archive?"
            f"latitude={loc['lat']}&longitude={loc['lon']}"
            f"&start_date={start_date}&end_date={end_date}"
            "&hourly=temperature_2m,precipitation"
            "&timezone=America/New_York"
        )

        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        session.sql("""
            INSERT INTO RAW.WEATHER_JSON (LOCATION_NAME, LOAD_TS, PAYLOAD)
            SELECT ?, CURRENT_TIMESTAMP(), PARSE_JSON(?)
        """, (loc["name"], json.dumps(data))).collect()

        inserted_rows += 1

    return f"✅ Loaded {inserted_rows} location(s) successfully."
$$;
