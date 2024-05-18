import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
import mysql.connector

# Connexion à la base de données
def fetch_weather_data():
    db_connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="shema"
    )
    query = """
    SELECT 
        d.date AS Date, 
        s.name AS Station, 
        s.latitude AS Latitude, 
        s.longitude AS Longitude, 
        f.PRCP AS Precipitation, 
        f.TAVG AS AvgTemp, 
        f.TMAX AS MaxTemp, 
        f.TMIN AS MinTemp
    FROM fact_temperature f
    JOIN date d ON f.id_date = d.id
    JOIN station s ON f.id_station = s.id
    """
    data = pd.read_sql(query, db_connection)
    db_connection.close()
    return data

# Initialisation de l'application Dash
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Mise en page de l'application
app.layout = html.Div(children=[
    html.H1(children='Tableau de Bord Météo', className='header-title'),

    html.Div(children=[
        dcc.Dropdown(
            id='station-dropdown',
            options=[],
            placeholder="Sélectionnez une station",
            multi=False,
            className='dropdown'
        )
    ], className='dropdown-container'),

    html.Div(children=[
        dcc.Graph(id='temperature-graph', className='graph')
    ], className='graph-container'),

    html.Div(children=[
        dcc.Graph(id='precipitation-graph', className='graph')
    ], className='graph-container'),

    html.Div(children=[
        html.P(
            "Données météo fournies par votre source de données. "
            "Créé avec ❤️ par Bouyemmout Meriem et Souci Nour el-Houda",
            className='footer'
        )
    ])
])

# Mise à jour des options du dropdown avec les stations disponibles
@app.callback(
    Output('station-dropdown', 'options'),
    [Input('station-dropdown', 'value')]
)
def set_station_options(selected_station):
    data = fetch_weather_data()
    stations = data['Station'].unique()
    return [{'label': station, 'value': station} for station in stations]

# Mise à jour des graphiques en fonction de la station sélectionnée
@app.callback(
    [Output('temperature-graph', 'figure'),
     Output('precipitation-graph', 'figure')],
    [Input('station-dropdown', 'value')]
)
def update_graphs(selected_station):
    data = fetch_weather_data()
    if selected_station:
        filtered_data = data[data['Station'] == selected_station]
    else:
        filtered_data = data

    temp_fig = px.line(filtered_data, x='Date', y=['AvgTemp', 'MaxTemp', 'MinTemp'], title=f'Températures à {selected_station}', labels={'value': 'Température (°C)', 'Date': 'Date'}, template='plotly_dark')
    precip_fig = px.bar(filtered_data, x='Date', y='Precipitation', title=f'Précipitations à {selected_station}', labels={'Precipitation': 'Précipitations (mm)', 'Date': 'Date'}, template='plotly_dark')

    return temp_fig, precip_fig

# Exécution du serveur
if __name__ == '__main__':
    app.run_server(debug=True)
