import os
import dash
import dash_bootstrap_components as dbc
from DatabricksChatbot import DatabricksChatbot
# Ensure environment variable is set correctly
serving_endpoint = os.getenv('SERVING_ENDPOINT')
assert serving_endpoint, 'SERVING_ENDPOINT must be set in app.yaml.'

# Initialize the Dash app with a clean theme
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.FLATLY])

# Create the chatbot component
chatbot = DatabricksChatbot(app=app, endpoint_name=serving_endpoint)

# Define the app layout
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col(chatbot.layout, width={'size': 8, 'offset': 2})
    ])
], fluid=True)

if __name__ == '__main__':
    # Databricks Apps sets PORT (same value as DATABRICKS_APP_PORT). Listen on all
    # interfaces so the managed proxy can reach the process.
    _port = int(os.environ.get('PORT', os.environ.get('DATABRICKS_APP_PORT', '8050')))
    _debug = os.environ.get('DATABRICKS_APP_NAME') is None
    app.run(host='0.0.0.0', port=_port, debug=_debug)
