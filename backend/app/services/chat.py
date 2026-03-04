"""Claude AI chat integration service.

Uses ANTHROPIC_API_KEY_CHAT — completely separate from the schema-agent
(services/schema_agent.py) which uses ANTHROPIC_API_KEY_AGENT.
"""

# TODO: Implement full chat logic
# This service should:
# - Accept a dataset_id, user message, and conversation history
# - Inject dataset schema context into the system prompt
# - Call the Anthropic API using ANTHROPIC_API_KEY_CHAT
# - Parse the response to optionally extract a structured QueryRequest
# - Return the assistant message and optional query

from app.config import settings  # noqa: F401  (used when implemented)


async def chat_with_data(dataset_id: str, message: str, history: list[dict]) -> dict:
    """Send a message to Claude (ANTHROPIC_API_KEY_CHAT) with dataset context.

    TODO: Implement this function.
    - Load dataset metadata (columns, types, sample values)
    - Build a system prompt describing the data schema
    - Append conversation history + new user message
    - Call anthropic.AsyncAnthropic(api_key=settings.ANTHROPIC_API_KEY_CHAT)
      .messages.create(...)
    - Optionally parse structured query from response
    - Return {"message": str, "suggested_query": QueryRequest | None}
    """
    raise NotImplementedError("chat_with_data is not yet implemented")
