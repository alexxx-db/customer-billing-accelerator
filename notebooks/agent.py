from typing import Any, Generator, Optional, Sequence, Union
import time

import mlflow
from databricks.sdk import WorkspaceClient
from databricks_langchain import (
    ChatDatabricks,
    VectorSearchRetrieverTool,
    DatabricksFunctionClient,
    UCFunctionToolkit,
    set_uc_function_client,
)
from langchain_core.language_models import LanguageModelLike
from langchain_core.runnables import RunnableConfig, RunnableLambda
from langchain_core.tools import BaseTool, tool
from langgraph.graph import END, StateGraph
from langgraph.graph.graph import CompiledGraph
from langgraph.graph.state import CompiledStateGraph
from langgraph.prebuilt.tool_node import ToolNode
from mlflow.langchain.chat_agent_langgraph import ChatAgentState, ChatAgentToolNode
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import (
    ChatAgentChunk,
    ChatAgentMessage,
    ChatAgentResponse,
    ChatContext,
)
from mlflow.models import ModelConfig

mlflow.langchain.autolog()

client = DatabricksFunctionClient()
set_uc_function_client(client)

config = ModelConfig(development_config="config.yaml").to_dict()


############################################
# Define your LLM endpoint and system prompt
############################################
# LLM_ENDPOINT_NAME = LLM_ENDPOINT
llm = ChatDatabricks(endpoint=config['llm_endpoint'])
system_prompt = config['agent_prompt']

###############################################################################
## Define tools for your agent, enabling it to retrieve data or take actions
## beyond text generation
## To create and see usage examples of more tools, see
## https://learn.microsoft.com/azure/databricks/generative-ai/agent-framework/agent-tool
###############################################################################
catalog = config['catalog']
schema = config['schema']

tools = []

# You can use UDFs in Unity Catalog as agent tools
uc_tool_names = [
    config['tools_billing_faq'],
    config['tools_billing'],
    config['tools_items'],
    config['tools_plans'],  
    config['tools_customer']
    ]
uc_toolkit = UCFunctionToolkit(function_names=uc_tool_names)
tools.extend(uc_toolkit.tools)

###############################################################################
## Genie Space tool for ad-hoc billing analytics
## This tool delegates complex analytical questions to a Databricks Genie Space
## that can generate and execute SQL over the full billing dataset.
###############################################################################
genie_space_id = config.get('genie_space_id', '')

if genie_space_id:
    _genie_client = WorkspaceClient()

    @tool
    def ask_billing_analytics(question: str) -> str:
        """Ask an ad-hoc billing analytics question using natural language.
        Use this tool for complex analytical questions that span multiple customers
        or require aggregations across the billing dataset, such as:
        - Revenue and charge trends over time
        - Plan comparisons and averages
        - Top-N customer rankings
        - Customer segmentation by charges or plan type
        - Month-over-month or period-over-period analysis

        Do NOT use this for individual customer lookups — use the dedicated
        lookup_customer, lookup_billing, or lookup_billing_items tools instead.
        """
        response = _genie_client.genie.start_conversation(
            space_id=genie_space_id,
            content=question,
        )

        conversation_id = response.conversation_id
        message_id = response.message_id

        # Poll for completion (Genie queries are async)
        max_attempts = 30
        result = None
        for _ in range(max_attempts):
            result = _genie_client.genie.get_message(
                space_id=genie_space_id,
                conversation_id=conversation_id,
                message_id=message_id,
            )
            if hasattr(result, 'status') and result.status in ("COMPLETED", "FAILED"):
                break
            time.sleep(2)

        if result is None or not hasattr(result, 'status'):
            return "The analytics query timed out. Please try a simpler question."

        if result.status == "FAILED":
            return "The analytics query could not be completed. Please try rephrasing your question."

        if result.status != "COMPLETED":
            return "The analytics query timed out. Please try a simpler question."

        # Extract results from attachments
        if hasattr(result, 'attachments') and result.attachments:
            parts = []
            for att in result.attachments:
                if hasattr(att, 'text') and att.text:
                    parts.append(att.text.content)
                if hasattr(att, 'query') and att.query:
                    if att.query.description:
                        parts.append(att.query.description)
                    if att.query.query:
                        parts.append(f"SQL: {att.query.query}")
            if parts:
                return "\n\n".join(parts)

        return "No results found for your analytics question."

    tools.append(ask_billing_analytics)

#####################
## Define agent logic
#####################


def create_tool_calling_agent(
    model: LanguageModelLike,
    tools: Union[Sequence[BaseTool], ToolNode],
    system_prompt: Optional[str] = None,
) -> CompiledGraph:
    model = model.bind_tools(tools)

    # Define the function that determines which node to go to
    def should_continue(state: ChatAgentState):
        messages = state["messages"]
        last_message = messages[-1]
        # If there are function calls, continue. else, end
        if last_message.get("tool_calls"):
            return "continue"
        else:
            return "end"

    system_message = [{"role": "system", "content": system_prompt}] if system_prompt else []

    def call_model(
        state: ChatAgentState,
        config: RunnableConfig,
    ):
        messages = state["messages"]
        # Only prepend system prompt if not already present
        if system_message and (not messages or messages[0].get("role") != "system"):
            messages = system_message + messages
        response = model.invoke(messages, config)

        return {"messages": [response]}

    workflow = StateGraph(ChatAgentState)

    workflow.add_node("agent", RunnableLambda(call_model))
    workflow.add_node("tools", ChatAgentToolNode(tools))

    workflow.set_entry_point("agent")
    workflow.add_conditional_edges(
        "agent",
        should_continue,
        {
            "continue": "tools",
            "end": END,
        },
    )
    workflow.add_edge("tools", "agent")

    return workflow.compile(recursion_limit=25)


class LangGraphChatAgent(ChatAgent):
    def __init__(self, agent: CompiledStateGraph):
        self.agent = agent

    def predict(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> ChatAgentResponse:
        request = {"messages": self._convert_messages_to_dict(messages)}

        messages = []
        for event in self.agent.stream(request, stream_mode="updates"):
            for node_data in event.values():
                messages.extend(
                    ChatAgentMessage(**msg) for msg in node_data.get("messages", [])
                )
        return ChatAgentResponse(messages=messages)

    def predict_stream(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> Generator[ChatAgentChunk, None, None]:
        request = {"messages": self._convert_messages_to_dict(messages)}
        for event in self.agent.stream(request, stream_mode="updates"):
            for node_data in event.values():
                yield from (
                    ChatAgentChunk(**{"delta": msg}) for msg in node_data.get("messages", [])
                )


# Create the agent object, and specify it as the agent object to use when
# loading the agent back for inference via mlflow.models.set_model()
agent = create_tool_calling_agent(llm, tools, system_prompt)
AGENT = LangGraphChatAgent(agent)
mlflow.models.set_model(AGENT)
