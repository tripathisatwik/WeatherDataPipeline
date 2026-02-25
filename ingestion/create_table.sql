CREATE TABLE IF NOT EXISTS weather_events_raw (
    id SERIAL PRIMARY KEY,
    event_time TIMESTAMP NOT NULL,
    latitude FLOAT,
    longitude FLOAT,
    raw_json JSONB
);

CREATE TABLE IF NOT EXISTS weather_events_clean (
    id SERIAL PRIMARY KEY,
    event_time TIMESTAMP NOT NULL,
    temperature FLOAT,
    windspeed FLOAT,
    winddirection FLOAT
);

CREATE TABLE IF NOT EXISTS ingestion_metrics (
    id SERIAL PRIMARY KEY,           
    run_time TIMESTAMPTZ NOT NULL,   
    rows_loaded INT NOT NULL,        
    status VARCHAR(20) DEFAULT 'SUCCESS' 
);

CREATE TABLE IF NOT EXISTS pipeline_failures (
    dag_id TEXT,
    task_id TEXT,
    run_id TEXT,
    error_message TEXT,
    failed_at TIMESTAMPTZ
);