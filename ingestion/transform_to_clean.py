import psycopg2

DB_CONFIG = {
    "host" : "postgres",
    "port" : 5432,
    "database" : "de_warehouse",
    "user" : "de_user",
    "password" : "de_pass" 
}

def transform():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    with open("/opt/airflow/ingestion/transform_raw_to_clean.sql","r") as f:
        cur.execute(f.read())
        conn.commit()

    cur.close()
    conn.close()
    print("Transformed raw data into clean analytics table")

if __name__ == "__main__":
    transform()