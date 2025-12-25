import os
import time
import pandas as pd
from sqlalchemy import create_engine, text
import dash
from dash import dcc, html, Input, Output
import plotly.express as px
from dash import dash_table

DB_HOST = os.getenv("DB_HOST", "postgres")
DATABASE_URL = f"postgresql://prefect:prefect@{DB_HOST}:5432/climate_db"

while True:
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 FROM climate_analytics LIMIT 1"))
            if result.fetchone() is not None:
                break
    except Exception as e:
        print()
    time.sleep(5)

df = pd.read_sql("SELECT * FROM climate_analytics", engine)
df = df.sort_values(by=["city", "year"])
cities = sorted(df["city"].dropna().unique())

app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Климатические данные", style={"textAlign": "center"}),
    
    html.Div([
        dcc.Dropdown(
            id="city-dropdown",
            options=[{"label": c, "value": c} for c in cities],
            value=["Moscow", "Paris", "New York City", "Addis Ababa"],
            multi=True
        ),
    ], style={'width': '80%', 'margin': 'auto'}),

    dcc.Graph(id="graph"),


    html.Div([
        html.Div([
            html.H3("Топ 10 самых жарких (год/город)", style={'textAlign': 'center', 'color': 'red'}),
            html.Div(id="hottest-table-container")
        ], style={'width': '48%', 'display': 'inline-block', 'verticalAlign': 'top'}),

        html.Div([
            html.H3("Топ 10 самых холодных (год/город)", style={'textAlign': 'center', 'color': 'blue'}),
            html.Div(id="coldest-table-container")
        ], style={'width': '48%', 'display': 'inline-block', 'float': 'right', 'verticalAlign': 'top'})
    ], style={'padding': '20px'})
])


@app.callback(
    [Output("graph", "figure"),
     Output("hottest-table-container", "children"),
     Output("coldest-table-container", "children")],
    [Input("city-dropdown", "value")]
)
def update_dashboard(selected_cities):
    if not selected_cities:
        return px.line(), "Выберите город", "Выберите город"
    
    filtered = df[df["city"].isin(selected_cities)]
    
    fig = px.line(filtered.sort_values(by="year"), 
                  x="year", y="avg_annual_temp", color="city",
                  title="Динамика температуры по годам")

    hottest_df = filtered.nlargest(10, "avg_annual_temp")[["city", "year", "avg_annual_temp"]]
    coldest_df = filtered.nsmallest(10, "avg_annual_temp")[["city", "year", "avg_annual_temp"]]

    def build_table(table_df):
        return dash_table.DataTable(
            data=table_df.to_dict('records'),
            columns=[
                {"name": "Город", "id": "city"},
                {"name": "Год", "id": "year"},
                {"name": "Темп. (C)", "id": "avg_annual_temp", "type": "numeric", "format": {"specifier": ".2f"}}
            ],
            style_cell={'textAlign': 'center', 'fontFamily': 'sans-serif'},
            style_header={'backgroundColor': '#f4f4f4', 'fontWeight': 'bold'},
            style_data_conditional=[{
                'if': {'row_index': 'odd'},
                'backgroundColor': 'rgb(248, 248, 248)'
            }]
        )

    return fig, build_table(hottest_df), build_table(coldest_df)

if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=8050, debug=False)