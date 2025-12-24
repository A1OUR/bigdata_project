-- raw_weather (уже есть)
CREATE TABLE IF NOT EXISTS raw_weather (
    city TEXT,
    country TEXT,
    date DATE,
    avg_temp_c FLOAT
);

-- НОВАЯ ТАБЛИЦА
CREATE TABLE IF NOT EXISTS climate_analytics (
    city TEXT,
    year INTEGER,
    avg_annual_temp FLOAT
);