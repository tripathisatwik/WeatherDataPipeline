INSERT INTO weather_events_clean (event_time, temperature, windspeed, winddirection)
SELECT
    event_time,
    (raw_json->'data'->'current_weather'->>'temperature')::FLOAT AS temperature,
    (raw_json->'data'->'current_weather'->>'windspeed')::FLOAT AS windspeed,
    (raw_json->'data'->'current_weather'->>'winddirection')::FLOAT AS winddirection
FROM weather_events_raw;