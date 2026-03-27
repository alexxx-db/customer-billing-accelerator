import time

import dash
from dash import html, Input, Output, State, dcc
import dash_bootstrap_components as dbc
from model_serving_utils import query_endpoint

# Rate limiting: minimum seconds between endpoint calls per session
_MIN_REQUEST_INTERVAL = 2.0
_MAX_INPUT_LENGTH = 2000

class DatabricksChatbot:
    def __init__(self, app, endpoint_name):
        self.app = app
        self.endpoint_name = endpoint_name
        self._last_request_time = 0.0
        self.layout = self._create_layout()
        self._create_callbacks()
        self._add_custom_css()

    def _create_layout(self):
        return html.Div([
            html.H2('Billing AI Assistant', className='chat-title mb-3'),
            html.Div([
                "Note: this is a simple example. See ",
                html.A("Databricks docs", href="https://docs.databricks.com/aws/en/generative-ai/agent-framework/chat-app", target="_blank"),
                " for a more comprehensive example, with support for streaming output and more."
            ]),
            # Persona selector
            html.Div([
                html.Label("Viewing as:", className="persona-label me-2"),
                dbc.RadioItems(
                    id="persona-selector",
                    className="persona-radio",
                    options=[
                        {"label": "Customer Support", "value": "customer_care"},
                        {"label": "Finance & Analytics", "value": "finance_ops"},
                        {"label": "Executive View", "value": "executive"},
                        {"label": "Platform Engineering", "value": "technical"},
                    ],
                    value="customer_care",
                    inline=True,
                ),
            ], className="persona-bar mb-2"),
            dcc.Store(id="persona-store", data="customer_care"),
            dbc.Card([
                dbc.CardBody([
                    html.Div(id='chat-history', className='chat-history'),
                ], className='d-flex flex-column chat-body')
            ], className='chat-card mb-3'),
            dbc.InputGroup([
                dbc.Input(id='user-input', placeholder='Type your message here...', type='text', maxLength=_MAX_INPUT_LENGTH),
                dbc.Button('Send', id='send-button', color='success', n_clicks=0, className='ms-2'),
                dbc.Button('Clear', id='clear-button', color='danger', n_clicks=0, className='ms-2'),
            ], className='mb-3'),
            dcc.Store(id='assistant-trigger'),
            dcc.Store(id='chat-history-store'),
            html.Div(id='dummy-output', style={'display': 'none'}),
        ], className='d-flex flex-column chat-container p-3')

    def _create_callbacks(self):
        # Persona switch: clear chat and update store
        @self.app.callback(
            Output("persona-store", "data"),
            Output("chat-history-store", "data", allow_duplicate=True),
            Output("chat-history", "children", allow_duplicate=True),
            Input("persona-selector", "value"),
            prevent_initial_call=True,
        )
        def switch_persona(persona_value):
            labels = {
                "customer_care": "Customer Support",
                "finance_ops": "Finance & Analytics",
                "executive": "Executive View",
                "technical": "Platform Engineering",
            }
            descs = {
                "customer_care": "I'll help with individual customer billing inquiries.",
                "finance_ops": "I'll provide fleet-wide billing analytics and finance operations intelligence.",
                "executive": "I'll deliver concise executive summaries of billing performance.",
                "technical": "I'll focus on platform health, pipeline diagnostics, and DBU cost analysis.",
            }
            label = labels.get(persona_value, persona_value)
            switch_msg = {
                "role": "assistant",
                "content": f"Switched to **{label}** mode. {descs.get(persona_value, '')}"
            }
            return persona_value, [switch_msg], self._format_chat_display([switch_msg])

        @self.app.callback(
            Output('chat-history-store', 'data', allow_duplicate=True),
            Output('chat-history', 'children', allow_duplicate=True),
            Output('user-input', 'value'),
            Output('assistant-trigger', 'data'),
            Input('send-button', 'n_clicks'),
            Input('user-input', 'n_submit'),
            State('user-input', 'value'),
            State('chat-history-store', 'data'),
            State('persona-store', 'data'),
            prevent_initial_call=True
        )
        def update_chat(send_clicks, user_submit, user_input, chat_history, persona):
            if not user_input:
                return dash.no_update, dash.no_update, dash.no_update, dash.no_update

            chat_history = chat_history or []
            chat_history.append({'role': 'user', 'content': user_input})
            chat_display = self._format_chat_display(chat_history)
            chat_display.append(self._create_typing_indicator())

            return chat_history, chat_display, '', {'trigger': True, 'persona': persona or 'customer_care'}

        @self.app.callback(
            Output('chat-history-store', 'data', allow_duplicate=True),
            Output('chat-history', 'children', allow_duplicate=True),
            Input('assistant-trigger', 'data'),
            State('chat-history-store', 'data'),
            prevent_initial_call=True
        )
        def process_assistant_response(trigger, chat_history):
            if not trigger or not trigger.get('trigger'):
                return dash.no_update, dash.no_update

            chat_history = chat_history or []
            if (not chat_history or not isinstance(chat_history[-1], dict)
                    or 'role' not in chat_history[-1]
                    or chat_history[-1]['role'] != 'user'):
                return dash.no_update, dash.no_update

            persona = trigger.get('persona', 'customer_care')

            try:
                assistant_response = self._call_model_endpoint(chat_history, persona=persona)
                chat_history.append({
                    'role': 'assistant',
                    'content': assistant_response
                })
            except Exception as e:
                print(f'Error calling model endpoint: {str(e)}')
                chat_history.append({
                    'role': 'assistant',
                    'content': 'Sorry, something went wrong processing your request. Please try again.'
                })

            chat_display = self._format_chat_display(chat_history)
            return chat_history, chat_display

        @self.app.callback(
            Output('chat-history-store', 'data', allow_duplicate=True),
            Output('chat-history', 'children', allow_duplicate=True),
            Input('clear-button', 'n_clicks'),
            prevent_initial_call=True
        )
        def clear_chat(n_clicks):
            if n_clicks:
                return [], []
            return dash.no_update, dash.no_update

    def _call_model_endpoint(self, messages, max_tokens=1024, persona="customer_care"):
        # Rate limiting: enforce minimum interval between requests
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < _MIN_REQUEST_INTERVAL:
            time.sleep(_MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.time()

        try:
            return query_endpoint(self.endpoint_name, messages, max_tokens,
                                  persona=persona)["content"]
        except Exception as e:
            print(f'Error calling model endpoint: {str(e)}')
            raise

    def _format_chat_display(self, chat_history):
        return [
            html.Div([
                html.Div(msg['content'],
                         className=f"chat-message {msg['role']}-message")
            ], className=f"message-container {msg['role']}-container")
            for msg in chat_history if isinstance(msg, dict) and 'role' in msg
        ]

    def _create_typing_indicator(self):
        return html.Div([
            html.Div(className='chat-message assistant-message typing-message',
                     children=[
                         html.Div(className='typing-dot'),
                         html.Div(className='typing-dot'),
                         html.Div(className='typing-dot')
                     ])
        ], className='message-container assistant-container')

    def _add_custom_css(self):
        custom_css = '''
        @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&display=swap');
        body {
            font-family: 'DM Sans', sans-serif;
            background-color: #F9F7F4;
        }
        .chat-container {
            max-width: 800px;
            margin: 0 auto;
            background-color: #FFFFFF;
            border-radius: 10px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            height: 100vh;
            display: flex;
            flex-direction: column;
        }
        .chat-title { font-size: 24px; font-weight: 700; color: #1B3139; text-align: center; }
        .chat-card { border: none; background-color: #EEEDE9; flex-grow: 1; display: flex; flex-direction: column; overflow: hidden; }
        .chat-body { flex-grow: 1; overflow: hidden; display: flex; flex-direction: column; }
        .chat-history { flex-grow: 1; overflow-y: auto; padding: 15px; }
        .message-container { display: flex; margin-bottom: 15px; }
        .user-container { justify-content: flex-end; }
        .chat-message { max-width: 80%; padding: 10px 15px; border-radius: 20px; font-size: 16px; line-height: 1.4; }
        .user-message { background-color: #FF3621; color: white; }
        .assistant-message { background-color: #1B3139; color: white; }
        .typing-message { background-color: #2D4550; color: #EEEDE9; display: flex; justify-content: center; align-items: center; min-width: 60px; }
        .typing-dot { width: 8px; height: 8px; background-color: #EEEDE9; border-radius: 50%; margin: 0 3px; animation: typing-animation 1.4s infinite ease-in-out; }
        .typing-dot:nth-child(1) { animation-delay: 0s; }
        .typing-dot:nth-child(2) { animation-delay: 0.2s; }
        .typing-dot:nth-child(3) { animation-delay: 0.4s; }
        @keyframes typing-animation { 0% { transform: translateY(0px); } 50% { transform: translateY(-5px); } 100% { transform: translateY(0px); } }
        #user-input { border-radius: 20px; border: 1px solid #DCE0E2; }
        #send-button, #clear-button { border-radius: 20px; width: 100px; }
        #send-button { background-color: #00A972; border-color: #00A972; }
        #clear-button { background-color: #98102A; border-color: #98102A; }
        .input-group { flex-wrap: nowrap; }
        .persona-bar { display: flex; align-items: center; padding: 8px 12px; background: #F5F5F5; border-radius: 8px; }
        .persona-label { font-size: 13px; font-weight: 500; color: #1B3139; white-space: nowrap; }
        .persona-radio .form-check { margin-right: 12px; }
        .persona-radio .form-check-label { font-size: 13px; cursor: pointer; }
        .persona-radio .form-check-input:checked { background-color: #FF3621; border-color: #FF3621; }
        '''
        self.app.index_string = self.app.index_string.replace(
            '</head>',
            f'<style>{custom_css}</style></head>'
        )

        self.app.clientside_callback(
            """
            function(children) {
                var chatHistory = document.getElementById('chat-history');
                if(chatHistory) {
                    chatHistory.scrollTop = chatHistory.scrollHeight;
                }
                return '';
            }
            """,
            Output('dummy-output', 'children'),
            Input('chat-history', 'children'),
            prevent_initial_call=True
        )
