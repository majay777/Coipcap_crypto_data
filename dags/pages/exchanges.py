import os
from datetime import datetime
from io import BytesIO

import boto3
import dash
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
from airflow.models import Variable
from dash import dcc, html, callback, dash_table
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate

s3_client = boto3.client('s3', endpoint_url=Variable.get("AWS_S3_ENDPOINT"),
                         aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
                         aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"))

# Download the JSON file from the MinIO bucket
bucket_name = 'bucket1'
date = datetime.today().strftime("%Y_%m_%d")
file_key = f'clean/exchanges/coincap_exchanges_{date}.json'

response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
data = response['Body'].read()
# Load the JSON data into a Pandas DataFrame
df = pd.read_json(BytesIO(data))

# df = pd.read_json("C:\\Users\\AJAY\\Downloads\\coincap_exchanges_2025_01_06.json")
df1 = df.fillna('N/A').replace('', 'N/A')
df['volumeUsd'] = '$' + (df['volumeUsd'].astype(float) / 1000000000).round(2).astype('str') + 'B'

dash.register_page(__name__)  # '/' is home page

card = dbc.Card(
    [
        dbc.CardBody(
            [
                html.H4("Exchanges by Volume", className='card-title'),

                html.P("Select Exchanges: ", className="card-text"
                       ),
                dcc.Dropdown(
                    id='select_exchange',
                    options=[{'label': i, 'value': i} for i in df['name'].unique()],
                    value='Binance', className="mb-4", style={'color': 'green'}
                ),

            ]
        )
    ], color="info", inverse=True,
)

# df['volumeUsd'] = '$' + (df['volumeUsd'].astype(float) / 1000000000).round(2).astype(str) + 'B'
# fig = px.pie(df, values='percentTotalVolume', names='name', hover_data='volumeUsd')
#
# fig.update_traces(textposition='inside', textinfo='percent+label')

layout = html.Div(

    [
        html.Div(card, id='page-content'),
        html.Div(dbc.Table(id='table')),
        dbc.Card(dbc.CardBody([html.H4("Exchanges", className="card-title"),
                               html.Div([
                                   dbc.Button("PIE", id="pie-chart", n_clicks=0, className='me-1', color='success'),
                                   dbc.Button("BAR", id="bar-graph", n_clicks=0, className='me-1', color='success'),
                               ]
                               ),
                               dcc.Graph(id='exchange-volume'),
                               ]), className="mb-3", )
    ]
)


@callback(Output("table", "children"),

          [Input("select_exchange", "value")])
def render_page_content(value):
    df2 = df1[df1['name'] == value]
    table = dash_table.DataTable(
        df2.to_dict('records'), [{"name": i, "id": i} for i in df2.columns],
        fixed_columns={'headers': True, 'data': 1},
        style_table={'minWidth': '100%'},
        style_as_list_view=True,
        style_cell={'padding': '5px'},
        style_header={

            'fontWeight': 'bold',
            'border': '1px solid pink',
            'backgroundColor': 'rgb(30, 30, 30)',
            'color': 'white'
        },
        style_data_conditional=[
            {
                'if': {
                    'filter_query': '{{{col}}} = "N/A"'.format(col=col),
                    'column_id': col
                },
                'backgroundColor': 'tomato',
                'color': 'white'
            } for col in df.columns
        ],
        style_data={
            'backgroundColor': 'rgb(80, 50, 80)',
            'color': 'white',
            'border': '1px solid pink',
        },
    )

    return table


@callback(Output("exchange-volume", 'figure'), [
    Input("pie-chart", "n_clicks"),
    Input("bar-graph", "n_clicks"),

])
def volume_graph(kpi_but, graph_but):
    if not kpi_but and not graph_but:
        raise PreventUpdate
    ctx = dash.callback_context
    button = ctx.triggered[0]["prop_id"].split(".")[0]
    fig_id = f"{button}_fig"

    if kpi_but:
        fig_pie = px.pie(df, values='percentTotalVolume', names='name', hover_data='volumeUsd')
        fig_pie.update_traces(textposition='inside', textinfo='percent+label')
    elif graph_but:
        fig_pie = px.bar(df, y='percentTotalVolume', x='name', hover_data='volumeUsd', color='name',
                         labels={'percentTotalVolume': 'Volume in %', 'name': 'Exchange Name'}, text_auto=True)
    else:
        raise PreventUpdate
    return fig_pie
