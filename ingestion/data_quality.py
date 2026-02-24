import psycopg2
from datetime import datetime, timezone

DB_CONFIG = {
    "host": "postgres",
    "database": "de_warehouse",
    "user": "de_user",
    "password": "de_pass",
    "port": 5432
}

def validate_and_log():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
            
    # 1. Row Count Check
    cur.execute("SELECT COUNT(*) FROM weather_events_clean;")
    total_rows = cur.fetchone()[0]
    
    if total_rows == 0:
        raise ValueError("Data Quality Failed: weather_events_clean is empty.")

    # 2. Null Check (Temperature)
    cur.execute("SELECT COUNT(*) FROM weather_events_clean WHERE temperature IS NULL;")
    null_temps = cur.fetchone()[0]

    if null_temps > 0:
        raise ValueError(f"Data Quality Failed: {null_temps} NULL temperatures found.")

    # 3. Range Check (Humidity)
    cur.execute("SELECT COUNT(*) FROM weather_events_clean WHERE humidity < 0 OR humidity > 100;")
    bad_humidity_count = cur.fetchone()[0]

    if bad_humidity_count > 0:
        raise ValueError(f"Data Quality Failed: {bad_humidity_count} rows have invalid humidity.")

    # 4. Log Metrics
    cur.execute("""
        INSERT INTO ingestion_metrics (run_time, rows_loaded, status)
        VALUES (%s, %s, %s)
    """, (datetime.now(timezone.utc), total_rows, 'SUCCESS'))
    
    conn.commit()
    print(f"Validation successful: {total_rows} rows verified and logged.")
    conn.close()
    cur.close()
    
if __name__ == "__main__":
    validate_and_log()