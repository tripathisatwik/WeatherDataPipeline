import json
import glob 
import psycopg2
from datetime import datetime

DB_CONFIG = {
    "host" : "localhost",
    "port" : 5432,
    "database" : "de_warehouse",
    "user" : "de_user",
    "password" : "de_pass" 
}

def load_files():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    with open("ingestion/create_table.sql","r") as f:
        cur.execute(f.read())
        conn.commit()

    files = glob.glob("data_lake/*.json")
    print(f"Found {len(files)} files")

    for file in files:
        with open(file,"r") as f:
            payload = json.load(f)

        event_time = payload["timestamp"]
        lat = payload["data"]["latitude"]
        lon = payload["data"]["longitude"]

        # Check if timestamp exists before insert to avoid unnecessary calls to api
        cur.execute(
            """
            INSERT INTO weather_events_raw (event_time, latitude, longitude, raw_json)
            SELECT %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM weather_events_raw WHERE event_time=%s
            )
            """,
            (event_time, lat, lon, json.dumps(payload), event_time)
        )
    
    conn.commit()
    conn.close()
    cur.close()
    print("Loaded raw events into PostgreSQL")

if __name__ == "__main__":
    load_files()