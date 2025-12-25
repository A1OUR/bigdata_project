import os
import pandas as pd
from prefect import flow, task
from sqlalchemy import create_engine, text
import dask.dataframe as dd

# === TASK 1: EXTRACT ===
@task
def extract(csv_path: str = "/workspace/data/city_temperature2005.csv"):
    """
    Читает CSV напрямую в Dask DataFrame — без загрузки в память.
    """
    df = dd.read_csv(csv_path, assume_missing=True, dtype={"State": "object"})  # State может быть пустым
    print(f"✅ Extracted CSV as Dask DataFrame (npartitions={df.npartitions})")
    return df

# === TASK 2: LOAD RAW ===
@task
def load_raw(df):
    """
    Фильтрует, преобразует и записывает данные в raw_weather партиями.
    """
    # Фильтрация некорректных дат
    df = df[
        (df["Day"] >= 1) &
        (df["Month"] >= 1) &
        (df["Month"] <= 12) &
        (df["Day"] <= 31)
    ]
    
    # Создание даты
    df["date"] = dd.to_datetime(df[["Year", "Month", "Day"]], errors="coerce")
    df = df.dropna(subset=["date"])
    
    # Температура в °C
    df["avg_temp_c"] = (df["AvgTemperature"] - 32) * 5 / 9
    
    # Выбор колонок
    df = df[["City", "Country", "date", "avg_temp_c"]].rename(
        columns={"City": "city", "Country": "country"}
    )
    
    # Подключение к БД
    db_host = os.getenv("DB_HOST", "localhost")
    db_url = f"postgresql://prefect:prefect@{db_host}:5432/climate_db"

    def write_partition(partition_df):
        if partition_df.empty:
            return 0
        worker_engine = create_engine(db_url)
        try:
            partition_df.to_sql(
                "raw_weather", 
                worker_engine, 
                if_exists="append", # Keep 'append' to respect existing table
                index=False, 
                chunksize=10000 
            )
        finally:
            worker_engine.dispose()
        return len(partition_df)

    print("🚀 Starting parallel write to Postgres...")
    rows_per_partition = df.map_partitions(write_partition).compute()
    
    total_rows = sum(rows_per_partition)
    print(f"✅ Loaded {total_rows} rows into raw_weather")
    return total_rows

# === TASK 3: TRANSFORM (Dask → агрегация) ===
@task
def transform_to_analytics():
    db_host = os.getenv("DB_HOST", "localhost")
    engine_url = f"postgresql://prefect:prefect@{db_host}:5432/climate_db"
    
    # Read using the numeric 'id' as the partition index
    df = dd.read_sql_table(
        "raw_weather",
        engine_url,
        index_col="id",
        npartitions=4
    )
    
    # Extract year from the date column
    df["year"] = dd.to_datetime(df["date"]).dt.year
    
    # Aggregate: Group by city and year
    result = df.groupby(["city", "year"])["avg_temp_c"].mean().reset_index()
    result = result.rename(columns={"avg_temp_c": "avg_annual_temp"})
    
    return result

@task
def load_analytics(df):
    db_host = os.getenv("DB_HOST", "localhost")
    engine = create_engine(f"postgresql://prefect:prefect@{db_host}:5432/climate_db")
    
    # Optional: Clear old analytics before appending new ones
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE climate_analytics"))
        conn.commit()
    
    df_computed = df.compute()
    df_computed.to_sql("climate_analytics", engine, if_exists="append", index=False)
    
    print(f"✅ Saved {len(df_computed)} rows to climate_analytics")
    return len(df_computed)

# === FLOW ===
@flow(name="Climate ETL Pipeline (Dask-Only)")
def climate_etl():
    df = extract()
    load_raw(df)
    analytics_df = transform_to_analytics()
    load_analytics(analytics_df)

if __name__ == "__main__":
    climate_etl()