import psycopg2

DB_CONFIG = {
    "host" : "localhost",
    "port" : 5432,
    "database" : "de_warehouse",
    "user" : "de_user",
    "password" : "de_pass" 
}

def check_data_quality():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    #Empty table check
    cur.execute("SELECT COUNT(*) FROM weather_events_clean")
    rows = cur.fetchone()[0]
    if rows == 0:
        print("Data Quality Check Failed: Cleaned Data Table is Empty")
    else:
        print("Data Quality Check Paseed: Cleaned Data Table has {rows} rows")

    cur.close()
    conn.close()

if __name__ == "__main__":
    check_data_quality()