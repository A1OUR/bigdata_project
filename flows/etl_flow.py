import os
from prefect import flow, task
from sqlalchemy import create_engine, text
import dask.dataframe as dd
import multiprocessing
import io
import psycopg2

@task
def extract(csv_path: str = "/workspace/data/city_temperature2005.csv"):
    df = dd.read_csv(csv_path, assume_missing=True, dtype={"State": "object"})
    return df

@task
def load_raw(df):
    df = df[
        (df["Day"] >= 1) &
        (df["Month"] >= 1) &
        (df["Month"] <= 12) &
        (df["Day"] <= 31)
    ]

    df["date"] = dd.to_datetime(df[["Year", "Month", "Day"]], errors="coerce")
    df = df.dropna(subset=["date"])
    
    df["avg_temp_c"] = (df["AvgTemperature"] - 32) * 5 / 9
    
    df = df[["City", "Country", "date", "avg_temp_c"]].rename(
        columns={"City": "city", "Country": "country"}
    )

    def write_partition(partition_df):
        if partition_df.empty:
            return 0
        
        db_host = os.getenv("DB_HOST", "localhost")
        conn = psycopg2.connect(
            host=db_host,
            database="climate_db",
            user="prefect",
            password="prefect"
        )
        
        try:
            cursor = conn.cursor()
            output = io.StringIO()
            partition_df.to_csv(output, sep='\t', header=False, index=False)
            output.seek(0)
            
            sql = "COPY raw_weather (city, country, date, avg_temp_c) FROM STDIN WITH (FORMAT csv, DELIMITER '\t')"
            cursor.copy_expert(sql, output)
            
            conn.commit()
            cursor.close()
        finally:
            conn.close()
            
        return len(partition_df)
        
    df.map_partitions(write_partition).compute()
    
@task
def transform_to_analytics():
    db_host = os.getenv("DB_HOST", "localhost")
    engine_url = f"postgresql://prefect:prefect@{db_host}:5432/climate_db"
    
    cores = multiprocessing.cpu_count()
    df = dd.read_sql_table(
        "raw_weather",
        engine_url,
        index_col="id",
        npartitions=cores
    )
    
    df["year"] = dd.to_datetime(df["date"]).dt.year
    
    result = df.groupby(["city", "year"])["avg_temp_c"].mean().reset_index()
    result = result.rename(columns={"avg_temp_c": "avg_annual_temp"})
    
    return result

@task
def load_analytics(df):
    db_host = os.getenv("DB_HOST", "localhost")
    engine = create_engine(f"postgresql://prefect:prefect@{db_host}:5432/climate_db")
    
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE climate_analytics"))
        conn.commit()
    
    df_computed = df.compute()
    df_computed.to_sql("climate_analytics", engine, if_exists="append", index=False)
    
    return len(df_computed)

@flow(name="Climate ETL Pipeline")
def climate_etl():
    df = extract()
    load_raw(df)
    analytics_df = transform_to_analytics()
    load_analytics(analytics_df)

if __name__ == "__main__":
    climate_etl()