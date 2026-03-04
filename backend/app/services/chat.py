"""Claude AI integration service."""

# TODO: Implement Claude AI chat integration
# This service should:
# - Accept a dataset_id, user message, and conversation history
# - Inject dataset schema context into the system prompt
# - Call the Anthropic API using the anthropic SDK
# - Parse the response to optionally extract a structured QueryRequest
# - Return the assistant message and optional query


async def chat_with_data(dataset_id: str, message: str, history: list[dict]) -> dict:
    """Send a message to Claude with dataset context and return a response.

    TODO: Implement this function.
    - Load dataset metadata (columns, types, sample values)
    - Build a system prompt describing the data schema
    - Append conversation history + new user message
    - Call anthropic.Anthropic().messages.create(...)
    - Optionally parse structured query from response
    - Return {"message": str, "suggested_query": QueryRequest | None}
    """
    raise NotImplementedError("chat_with_data is not yet implemented")
