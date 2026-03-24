from mlflow.deployments import get_deploy_client


def _query_endpoint(endpoint_name: str, messages: list[dict[str, str]],
                    max_tokens: int, persona: str = "customer_care") -> list[dict[str, str]]:
    """Calls a model serving endpoint with persona context via custom_inputs."""
    res = get_deploy_client('databricks').predict(
        endpoint=endpoint_name,
        inputs={
            'messages': messages,
            "max_tokens": max_tokens,
            "custom_inputs": {"persona": persona},
        },
    )
    if "messages" in res:
        return res["messages"]
    elif "choices" in res:
        return [res["choices"][0]["message"]]
    raise Exception(
        "This app can only run against: "
        "1) Databricks foundation model or external model endpoints with the chat task type, or "
        "2) Databricks agent serving endpoints that implement the conversational agent schema."
    )


def query_endpoint(endpoint_name, messages, max_tokens, persona="customer_care"):
    return _query_endpoint(endpoint_name, messages, max_tokens, persona=persona)[-1]
