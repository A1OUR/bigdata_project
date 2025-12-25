CREATE TABLE IF NOT EXISTS raw_weather (
        id SERIAL PRIMARY KEY,
        city TEXT,
        country TEXT,
        date DATE,
        avg_temp_c FLOAT
);

CREATE TABLE IF NOT EXISTS climate_analytics (
    city TEXT,
    year INTEGER,
    avg_annual_temp FLOAT
);