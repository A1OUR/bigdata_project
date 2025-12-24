import os
import pandas as pd
from prefect import flow, task
from sqlalchemy import create_engine

@task
def load_raw_data():
    # 1. Читаем CSV
    df = pd.read_csv("/workspace/data/city_temperature2005.csv", low_memory=False)

    # 2. Удаляем строки с некорректными датами (Day=0, Month=0 и т.д.)
    #    В исходном датасете Day=0 означает отсутствие данных
    df = df[(df["Day"] >= 1) & (df["Month"] >= 1) & (df["Month"] <= 12) & (df["Day"] <= 31)]

    # 3. Создаём дату — с обработкой ошибок
    #    (на случай, если остались некорректные комбинации, например, 31 апреля)
    df["date"] = pd.to_datetime(
        df[["Year", "Month", "Day"]],
        errors="coerce"  # ← Некорректные даты → NaT
    )

    # 4. Удаляем строки, где дата не распознана
    df = df.dropna(subset=["date"])

    # 5. Переводим температуру в Цельсии
    df["avg_temp_c"] = (df["AvgTemperature"] - 32) * 5 / 9

    # 6. Оставляем нужные колонки
    df = df[["City", "Country", "date", "avg_temp_c"]].rename(columns={
        "City": "city",
        "Country": "country"
    })

    # 7. Сохраняем в PostgreSQL
    db_host = os.getenv("DB_HOST", "localhost")
    engine = create_engine(f"postgresql://prefect:prefect@{db_host}:5432/climate_db")
    df.to_sql("raw_weather", engine, if_exists="replace", index=False)
    print(f"✅ Загружено {len(df)} корректных строк в raw_weather")

@flow(name="Load Raw Weather Data")
def load_flow():
    load_raw_data()

if __name__ == "__main__":
    load_flow()