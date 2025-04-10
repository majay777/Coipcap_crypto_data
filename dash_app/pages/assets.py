import logging
from datetime import datetime

import dash
import dash_bootstrap_components as dbc
import pandas as pd
from dash import dcc, html, callback, dash_table
from dash.dependencies import Input, Output
from minio import Minio

bucket_name = 'bucket1'
date = datetime.today().strftime("%Y_%m_%d")
file_key = f'clean/assets/coincap_assets_{date}.json'

file_key2 = f"clean/bitcoin/bitcoin_markets_{date}.json"

client = Minio(str('127.0.0.1:9000'),
               access_key='minioadmin',
               secret_key='minioadmin',
               secure=False
               )

# Get data of an object.
try:
    response = client.get_object(bucket_name, file_key)
    # Load the JSON data into a Pandas DataFrame
    df = pd.read_json(response)

except ValueError as e:
    logging.error(e)

try:
    response2 = client.get_object(bucket_name, file_key2)

    market_df = pd.read_json(response2)
except ValueError as e:
    logging.error(e)

df = df.fillna('N/A').replace('', 'N/A')
dash.register_page(__name__)  # '/' is home page

card = dbc.Card(
    [
        dbc.CardBody(
            [
                html.H4("Crypto Coins", className='card-title'),
                html.P("Select Coins: ", className="card-text"
                       ),
                dcc.Dropdown(
                    id='select_coin',
                    options=[{'label': i, 'value': i} for i in df['name'].unique()],
                    value='Bitcoin', className="mb-4", style={'color': 'green'}
                ),

            ]
        )
    ], color="info", inverse=True,
)

card_tot_vol = dbc.Card(
    [

        dbc.CardBody(
            [
                html.H4("Total Volume in USD", className="card-title", style={'color': 'blue'}),

                html.H3("$", className='card-text', id='total_volume')

            ]
        ),
        dbc.CardFooter("Total volume in last 24 hour", style={'color': 'white'}),
    ],
    style={"width": "20rem"}, color="dark", inverse=True
)

card_market = dbc.Card(
    [

        dbc.CardBody(
            [
                html.H4("Market Cap", className="card-title", style={'color': 'blue'}),

                html.H3('$', className='card-text', id='market_cap')

            ]
        ),
        dbc.CardFooter("", style={'color': 'white'}, id='percent_change_footer'),
    ],
    style={"width": "20rem"}, color="dark", inverse=True
)

layout = html.Div(

    [
        html.Div(card, id='page-content-assets'),
        html.Br(),
        dbc.Row(
            [
                dbc.Col(card_market),
                dbc.Col(card_tot_vol)
            ],
            className="mb-4",
        ),
        html.Div(dbc.Table(id='table2')),
        html.Div([dbc.Table(id='market_tbl'),

                  ])
    ]
)


@callback([Output("table2", "children"),
           Output("market_tbl", "children"),
           Output('market_cap', 'children'),
           Output("percent_change_footer", "children"),
           Output("total_volume", "children")],
          [Input("select_coin", "value")])
def render_page_content(value):
    df1 = df[df['name'] == value]
    table = dash_table.DataTable(
        data=df1.to_dict('records'),
        columns=[{'id': c, 'name': c} for c in df1.columns],
        fixed_columns={'headers': True, 'data': 1},
        style_table={'minWidth': '100%'},
        style_header={
            'backgroundColor': 'rgb(150, 75, 100)',
            'color': 'white'
        },
        style_data={
            'backgroundColor': 'rgb(50, 40, 120)',
            'color': 'white'
        },
        style_data_conditional=[
            {
                'if': {
                    'filter_query': '{changePercent24Hr}  < 0',
                    'column_id': 'changePercent24Hr'
                },
                'color': 'red',
                'fontWeight': 'bold',
                'backgroundColor': 'white'
            },

            {
                'if': {
                    'filter_query': '{changePercent24Hr}  > 0',
                    'column_id': 'changePercent24Hr'
                },
                'color': 'green',
                'fontWeight': 'bold',
                'backgroundColor': 'white'

            }, ]
    )
    market_df1 = market_df[market_df['baseSymbol'] == df1['symbol'].values[0]]
    market_tbl = dash_table.DataTable(data=market_df1.to_dict('records'),
                                      columns=[{'id': c, 'name': c} for c in market_df1.columns],
                                      fixed_columns={'headers': True, 'data': 1},
                                      style_table={'minWidth': '100%'},
                                      style_header={
                                          'backgroundColor': 'rgb(40, 75, 100)',
                                          'color': 'white'
                                      },
                                      style_data={
                                          'backgroundColor': 'rgb(100, 40, 120)',
                                          'color': 'white'
                                      },
                                      page_action="native",
                                      page_current=0,
                                      page_size=10,

                                      )
    market_cap_usd = df1['marketCapUsd'].values[0]
    total_vol = df1['volumeUsd24Hr'].sum()
    percent_change = df1['changePercent24Hr'].values[0]
    return table, market_tbl, market_cap_usd, percent_change, total_vol
