import os
import pandas as pd
from prefect import flow, task
from sqlalchemy import create_engine
import dask.dataframe as dd

# === TASK 1: EXTRACT ===
@task
def extract(csv_path: str = "/workspace/data/city_temperature2005.csv"):
    df = pd.read_csv(csv_path)
    print(f"📄 Прочитано {len(df)} строк из CSV")
    return df

# === TASK 2: LOAD RAW ===
@task
def load_raw(df):
    # Фильтрация некорректных дат (Day=0, Month=0 и т.д.)
    df = df[(df["Day"] >= 1) & (df["Month"] >= 1) & (df["Month"] <= 12) & (df["Day"] <= 31)]
    
    # Создание корректной даты
    df["date"] = pd.to_datetime(df[["Year", "Month", "Day"]], errors="coerce")
    df = df.dropna(subset=["date"])
    
    # Перевод температуры в °C (колонка называется AvgTemperature!)
    df["avg_temp_c"] = (df["AvgTemperature"] - 32) * 5 / 9

    # Подключение к БД
    db_host = os.getenv("DB_HOST", "localhost")
    engine = create_engine(f"postgresql://prefect:prefect@{db_host}:5432/climate_db")
    
    # Сохранение в raw_weather
    df[["City", "Country", "date", "avg_temp_c"]].rename(
        columns={"City": "city", "Country": "country"}
    ).to_sql("raw_weather", engine, if_exists="replace", index=False)
    
    print(f"✅ Загружено {len(df)} строк в raw_weather")

# === TASK 3: TRANSFORM (Dask) ===
@task
def transform_to_analytics():
    db_host = os.getenv("DB_HOST", "localhost")
    engine = create_engine(f"postgresql://prefect:prefect@{db_host}:5432/climate_db")
    
    # 1. Читаем ВСЕ данные через pandas
    query = "SELECT city, date, avg_temp_c FROM raw_weather"
    df_pd = pd.read_sql(query, engine)
    
    # 2. Конвертируем в Dask DataFrame
    df = dd.from_pandas(df_pd, npartitions=4)
    
    # 3. Обработка
    df["year"] = df["date"].dt.year
    result = df.groupby(["city", "year"])["avg_temp_c"].mean().reset_index()
    result = result.rename(columns={"avg_temp_c": "avg_annual_temp"})
    
    return result.compute()

# === TASK 4: LOAD ANALYTICS ===
@task
def load_analytics(df):
    db_host = os.getenv("DB_HOST", "localhost")
    engine = create_engine(f"postgresql://prefect:prefect@{db_host}:5432/climate_db")
    df.to_sql("climate_analytics", engine, if_exists="replace", index=False)
    print(f"📈 Сохранено {len(df)} записей в climate_analytics")

# === FLOW ===
@flow(name="Climate ETL Pipeline")
def climate_etl():
    df = extract()
    load_raw(df)
    analytics_df = transform_to_analytics()
    load_analytics(analytics_df)

if __name__ == "__main__":
    climate_etl()