import os
import time
import pandas as pd
from sqlalchemy import create_engine, text
import dash
from dash import dcc, html, Input, Output
import plotly.express as px

DB_HOST = os.getenv("DB_HOST", "postgres")
DATABASE_URL = f"postgresql://prefect:prefect@{DB_HOST}:5432/climate_db"

print("⏳ Dash: ожидание данных в climate_analytics...")

while True:
    try:
        engine = create_engine(DATABASE_URL)
        # Используем text() для совместимости с SQLAlchemy 2.0+
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 FROM climate_analytics LIMIT 1"))
            if result.fetchone() is not None:
                print("✅ Dash: данные найдены. Запуск сервера...")
                break
    except Exception as e:
        print(f"   └─ Ожидание данных... ({type(e).__name__})")
    time.sleep(5)

# Загрузка данных
df = pd.read_sql("SELECT * FROM climate_analytics", engine)
cities = sorted(df["city"].dropna().unique())

app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("🌍 Климатические данные", style={"textAlign": "center"}),
    dcc.Dropdown(
        id="city-dropdown",
        options=[{"label": c, "value": c} for c in cities],
        value=cities[:3],
        multi=True
    ),
    dcc.Graph(id="graph")
])

@app.callback(Output("graph", "figure"), Input("city-dropdown", "value"))
def update_graph(cities):
    if not cities:
        return px.line()
    filtered = df[df["city"].isin(cities)]
    return px.line(filtered, x="year", y="avg_annual_temp", color="city")

if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=8050, debug=False)