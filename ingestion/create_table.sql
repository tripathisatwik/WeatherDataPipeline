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