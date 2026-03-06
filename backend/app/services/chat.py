"""Claude AI chat service with tool use and SSE streaming.

Uses ANTHROPIC_API_KEY_CHAT — completely separate from the schema-agent
(services/schema_agent.py) which uses ANTHROPIC_API_KEY_AGENT.

Architecture
~~~~~~~~~~~~
1.  ``build_system_prompt`` turns the dataset schema into a concise XML
    context block (< 4 000 tokens).
2.  Three tools are exposed to Claude:
    - **query_data** – run a grouped aggregation query
    - **create_scenario_rule** – build a what-if rule + impact preview
    - **list_dimension_values** – look up unique column values
3.  ``stream_chat`` is an async generator that yields typed SSE events
    (text_delta, tool_executing, tool_result, scenario_rule, done, error).
    The tool-use loop runs up to 3 rounds so Claude can call a tool,
    inspect the result, and respond.
"""

from __future__ import annotations

import json
import logging
import math
from typing import Any, AsyncGenerator

import anthropic

from app.config import settings
from app.database import sync_engine
from app.services import storage as storage_svc

logger = logging.getLogger(__name__)

# Max tool-use round-trips per chat turn
_MAX_TOOL_ROUNDS = 3

# Claude model for chat
_CHAT_MODEL = "claude-sonnet-4-6"

# ---------------------------------------------------------------------------
# Tool definitions (Anthropic tool-use format)
# ---------------------------------------------------------------------------

TOOLS = [
    {
        "name": "query_data",
        "description": (
            "Query the user's dataset.  Groups by the given columns and "
            "sums/averages the value column.  Returns max 50 rows.  "
            "Use this when the user asks about totals, breakdowns, "
            "comparisons, or to find specific values."
        ),
        "input_schema": {
            "type": "object",
            "required": ["group_by", "value_column"],
            "properties": {
                "group_by": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Columns to group by",
                },
                "value_column": {
                    "type": "string",
                    "description": "Numeric column to aggregate",
                },
                "aggregation": {
                    "type": "string",
                    "enum": ["sum", "avg", "min", "max", "count"],
                    "description": "Aggregation function (default: sum)",
                },
                "filters": {
                    "type": "object",
                    "description": (
                        "Filter criteria.  Each key is a column name; "
                        "the value is a list of allowed values."
                    ),
                    "additionalProperties": {
                        "type": "array",
                        "items": {},
                    },
                },
            },
        },
    },
    {
        "name": "create_scenario_rule",
        "description": (
            "Create a what-if scenario rule.  Returns the rule object "
            "plus an impact preview (rows affected, estimated delta).  "
            "Use this when the user asks 'what if costs increase by X%' "
            "or 'add 100K to budget'."
        ),
        "input_schema": {
            "type": "object",
            "required": ["name", "type"],
            "properties": {
                "name": {
                    "type": "string",
                    "description": "Short human-readable name for this rule",
                },
                "type": {
                    "type": "string",
                    "enum": ["multiplier", "offset"],
                },
                "factor": {
                    "type": "number",
                    "description": "Multiplication factor (e.g. 1.05 = +5%).  Required when type=multiplier.",
                },
                "offset": {
                    "type": "number",
                    "description": "Total amount to add/subtract.  Required when type=offset.",
                },
                "filters": {
                    "type": "object",
                    "description": "Restrict rule to matching rows.  Keys are column names; values are lists of allowed values.",
                    "additionalProperties": {
                        "type": "array",
                        "items": {},
                    },
                },
                "periodFrom": {
                    "type": "string",
                    "description": "Start period (YYYY-MM or YYYY-MM-DD).  Optional.",
                },
                "periodTo": {
                    "type": "string",
                    "description": "End period (YYYY-MM or YYYY-MM-DD).  Optional.",
                },
            },
        },
    },
    {
        "name": "list_dimension_values",
        "description": (
            "Look up the unique values for a column (dimension).  "
            "Use this to verify column values before building filters, "
            "or when the user asks 'what categories exist?'"
        ),
        "input_schema": {
            "type": "object",
            "required": ["column_name"],
            "properties": {
                "column_name": {
                    "type": "string",
                    "description": "Column to inspect",
                },
                "search": {
                    "type": "string",
                    "description": "Optional substring filter (case-insensitive)",
                },
            },
        },
    },
]


# ---------------------------------------------------------------------------
# System prompt builder
# ---------------------------------------------------------------------------

def build_system_prompt(schema_info: dict) -> str:
    """Create a concise system prompt from dataset schema metadata.

    ``schema_info`` has the shape::

        {
            "dataset_name": str,
            "table_name": str,
            "row_count": int,
            "columns": [
                {
                    "column_name": str,
                    "display_name": str,
                    "data_type": str,
                    "column_role": str,
                    "unique_count": int,
                    "sample_values": list,
                    "stats": {"min": ..., "max": ..., "sum": ...} | None,
                }
            ],
        }
    """
    parts: list[str] = []
    parts.append(
        "You are a financial data analyst assistant for DataBobIQ.  "
        "You help users explore ERP/accounting data, build what-if scenarios, "
        "and understand their numbers.  Be concise and direct.  "
        "Always use the provided tools to query actual data — never guess numbers."
    )

    parts.append("\n<dataset>")
    parts.append(f"  <name>{schema_info['dataset_name']}</name>")
    parts.append(f"  <table>{schema_info['table_name']}</table>")
    parts.append(f"  <rows>{schema_info['row_count']}</rows>")

    # Dimensions / keys
    dims = [c for c in schema_info["columns"] if c["column_role"] in ("key", "attribute", "time")]
    if dims:
        parts.append("  <dimensions>")
        for col in dims:
            samples = col.get("sample_values") or []
            top_vals = ", ".join(str(v) for v in samples[:15])
            parts.append(
                f"    <col name=\"{col['column_name']}\" display=\"{col['display_name']}\" "
                f"type=\"{col['data_type']}\" role=\"{col['column_role']}\" "
                f"unique=\"{col.get('unique_count', '?')}\">{top_vals}</col>"
            )
        parts.append("  </dimensions>")

    # Measures
    measures = [c for c in schema_info["columns"] if c["column_role"] == "measure"]
    if measures:
        parts.append("  <measures>")
        for col in measures:
            stats = col.get("stats") or {}
            stat_str = " ".join(f'{k}="{v}"' for k, v in stats.items())
            parts.append(
                f"    <col name=\"{col['column_name']}\" display=\"{col['display_name']}\" "
                f"type=\"{col['data_type']}\" {stat_str}/>"
            )
        parts.append("  </measures>")

    parts.append("</dataset>")

    parts.append(
        "\n<instructions>"
        "\n- When the user asks a factual question about their data, use query_data."
        "\n- When the user asks 'what if', create a scenario rule with create_scenario_rule."
        "\n- When you need to verify column values before filtering, use list_dimension_values."
        "\n- If the user's question is ambiguous, ask for clarification."
        "\n- Format numbers with thousand separators (e.g., 1,234,567)."
        "\n- If discussing German data, respond in German."
        "\n</instructions>"
    )

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Tool execution
# ---------------------------------------------------------------------------

async def execute_tool(
    tool_name: str,
    tool_input: dict,
    table_name: str,
    dataset_id: str = "",
) -> dict[str, Any]:
    """Execute a tool call and return the result dict.

    Runs synchronously against the sync_engine for data queries.
    Returns a dict ready to be JSON-serialised.
    """
    logger.info("Executing tool %s with input %s", tool_name, tool_input)

    if tool_name == "query_data":
        return _tool_query_data(tool_input, table_name)
    elif tool_name == "create_scenario_rule":
        return _tool_create_scenario_rule(tool_input, table_name)
    elif tool_name == "list_dimension_values":
        return _tool_list_dimension_values(tool_input, table_name, dataset_id=dataset_id)
    else:
        return {"error": f"Unknown tool: {tool_name}"}


def _tool_query_data(inp: dict, table_name: str) -> dict:
    """Run a grouped aggregation query and return ≤ 50 rows."""
    import polars as pl

    group_by: list[str] = inp.get("group_by", [])
    value_column: str = inp["value_column"]
    agg: str = inp.get("aggregation", "sum")
    filters: dict = inp.get("filters") or {}

    df = storage_svc.read_dataset(sync_engine, table_name)

    # Apply filters
    for col_name, values in filters.items():
        if col_name in df.columns and isinstance(values, list):
            str_vals = [str(v) for v in values]
            df = df.filter(pl.col(col_name).cast(pl.Utf8).is_in(str_vals))

    if value_column not in df.columns:
        return {"error": f"Column '{value_column}' not found in dataset"}

    valid_gb = [c for c in group_by if c in df.columns]

    agg_map = {"sum": "sum", "avg": "mean", "mean": "mean",
               "min": "min", "max": "max", "count": "count"}
    agg_fn = agg_map.get(agg, "sum")

    if valid_gb:
        result = (
            df.lazy()
            .group_by(valid_gb)
            .agg(getattr(pl.col(value_column).cast(pl.Float64), agg_fn)().alias(value_column))
            .sort(value_column, descending=True)
            .head(50)
            .collect()
        )
    else:
        val = getattr(df[value_column].cast(pl.Float64), agg_fn)()
        result = pl.DataFrame({value_column: [val]})

    rows = []
    for row in result.iter_rows(named=True):
        rows.append({k: _safe(v) for k, v in row.items()})

    return {
        "columns": result.columns,
        "rows": rows,
        "row_count": len(rows),
        "aggregation": agg,
    }


def _tool_create_scenario_rule(inp: dict, table_name: str) -> dict:
    """Build a scenario rule dict and compute an impact preview."""
    import polars as pl

    rule: dict[str, Any] = {
        "id": None,  # assigned on persist
        "name": inp.get("name", "Unnamed rule"),
        "type": inp.get("type", "multiplier"),
        "filters": inp.get("filters") or {},
    }
    if rule["type"] == "multiplier":
        rule["factor"] = float(inp.get("factor", 1.0))
    else:
        rule["offset"] = float(inp.get("offset", 0.0))

    if inp.get("periodFrom"):
        rule["periodFrom"] = inp["periodFrom"]
    if inp.get("periodTo"):
        rule["periodTo"] = inp["periodTo"]

    # Impact preview: count affected rows and estimate delta
    try:
        df = storage_svc.read_dataset(sync_engine, table_name)

        mask = pl.lit(True)
        for col_name, values in rule.get("filters", {}).items():
            if col_name in df.columns and isinstance(values, list):
                str_vals = [str(v) for v in values]
                mask = mask & pl.col(col_name).cast(pl.Utf8).is_in(str_vals)

        matched = df.filter(mask)
        affected_rows = len(matched)

        # Try to estimate delta on the first numeric column
        numeric_cols = [
            c for c in matched.columns
            if matched[c].dtype in (pl.Float32, pl.Float64, pl.Int32, pl.Int64)
               and c != "_row_id"
        ]
        estimated_delta = None
        if numeric_cols:
            col = numeric_cols[0]
            current_sum = float(matched[col].cast(pl.Float64).sum())
            if rule["type"] == "multiplier":
                estimated_delta = current_sum * (rule["factor"] - 1)
            else:
                estimated_delta = rule["offset"]

        rule["_preview"] = {
            "affected_rows": affected_rows,
            "total_rows": len(df),
            "estimated_delta": round(estimated_delta, 2) if estimated_delta is not None else None,
        }
    except Exception as exc:
        logger.warning("Impact preview failed: %s", exc)
        rule["_preview"] = {"error": str(exc)}

    return rule


def _tool_list_dimension_values(inp: dict, table_name: str, dataset_id: str = "") -> dict:
    """Return up to 100 unique values for a column, with semantic labels when available."""
    import polars as pl
    from sqlalchemy import text

    col_name: str = inp["column_name"]
    search: str | None = inp.get("search")

    df = storage_svc.read_dataset(sync_engine, table_name, columns=[col_name])

    if col_name not in df.columns:
        return {"error": f"Column '{col_name}' not found"}

    series = df[col_name].drop_nulls().unique().sort()

    if search:
        search_lower = search.lower()
        series = series.filter(series.cast(pl.Utf8).str.to_lowercase().str.contains(search_lower))

    raw_values = series.head(100).to_list()

    # Load semantic labels for these values if dataset_id is known
    label_map: dict[str, dict] = {}
    if dataset_id:
        try:
            with sync_engine.connect() as conn:
                rows = conn.execute(
                    text("""
                        SELECT svl.raw_value, svl.display_label, svl.category
                        FROM semantic_value_labels svl
                        JOIN semantic_columns sc ON sc.id = svl.semantic_column_id
                        WHERE sc.dataset_id = :ds_id AND sc.column_name = :col_name
                    """),
                    {"ds_id": dataset_id, "col_name": col_name},
                ).fetchall()
                for row in rows:
                    label_map[str(row[0])] = {"label": row[1], "category": row[2]}
        except Exception as exc:
            logger.warning("Failed to load semantic labels for %s: %s", col_name, exc)

    values_with_labels = []
    for v in raw_values:
        raw_str = str(_safe(v))
        entry: dict[str, Any] = {"value": _safe(v)}
        if raw_str in label_map:
            entry["label"] = label_map[raw_str]["label"]
            if label_map[raw_str]["category"]:
                entry["category"] = label_map[raw_str]["category"]
        values_with_labels.append(entry)

    return {
        "column_name": col_name,
        "values": values_with_labels,
        "total_unique": df[col_name].n_unique(),
        "showing": len(raw_values),
    }


def _safe(v: Any) -> Any:
    """Make a value JSON-serialisable."""
    from datetime import date, datetime
    if isinstance(v, float) and math.isnan(v):
        return None
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return v


# ---------------------------------------------------------------------------
# Schema info loader (used by routes.py before calling stream_chat)
# ---------------------------------------------------------------------------

def load_schema_info(dataset) -> dict:
    """Build the schema_info dict from a Dataset ORM object (with columns loaded).

    Optionally enriches measure columns with min/max/sum stats by querying
    the dynamic table.
    """
    import polars as pl

    columns_info: list[dict] = []
    measure_cols: list[str] = []

    for col in dataset.columns:
        info: dict[str, Any] = {
            "column_name": col.column_name,
            "display_name": col.display_name,
            "data_type": col.data_type,
            "column_role": col.column_role,
            "unique_count": col.unique_count,
            "sample_values": col.sample_values or [],
            "stats": None,
        }
        if col.column_role == "measure":
            measure_cols.append(col.column_name)
        columns_info.append(info)

    # Enrich measures with basic stats
    if measure_cols:
        try:
            df = storage_svc.read_dataset(sync_engine, dataset.table_name, columns=measure_cols)
            for info in columns_info:
                if info["column_role"] == "measure" and info["column_name"] in df.columns:
                    series = df[info["column_name"]].cast(pl.Float64, strict=False).drop_nulls()
                    if len(series) > 0:
                        info["stats"] = {
                            "min": round(float(series.min()), 2),
                            "max": round(float(series.max()), 2),
                            "sum": round(float(series.sum()), 2),
                        }
        except Exception as exc:
            logger.warning("Failed to enrich measure stats: %s", exc)

    return {
        "dataset_name": dataset.name,
        "table_name": dataset.table_name,
        "row_count": dataset.row_count,
        "columns": columns_info,
    }


# ---------------------------------------------------------------------------
# SSE streaming chat
# ---------------------------------------------------------------------------

def _build_system_prompt_from_context(context: str) -> str:
    """Build the chat system prompt incorporating the rich semantic context."""
    return (
        "You are a data analyst assistant for DataBobIQ. You help users explore data, "
        "build what-if scenarios, and understand their numbers. Be concise and direct.\n"
        "Always use the provided tools to query actual data — never guess numbers.\n\n"
        f"{context}\n\n"
        "<instructions>\n"
        "- When the user asks about their data, use query_data with the correct column names.\n"
        "- When you need to filter by a dimension, look at the value labels in the context above.\n"
        "  For example, if the user says 'personnel costs', find which column values correspond\n"
        "  to that from the labels/categories, then use those raw values in your filter.\n"
        "- IMPORTANT: If existing_groupings are documented, ALWAYS prefer filtering on those\n"
        "  grouping columns (e.g. reporting_h2, account_group) rather than raw ID/code ranges.\n"
        "- When the user asks 'what if', create a scenario rule with create_scenario_rule.\n"
        "- When you need to verify values, use list_dimension_values.\n"
        "- If the data context includes scenario_hints, follow those instructions.\n"
        "- Format numbers with thousand separators.\n"
        "- If the data appears to be German, respond in German when the user writes in German.\n"
        "</instructions>"
    )


def _resolve_table_name(dataset_id: str) -> str:
    """Resolve the dynamic table name for a dataset_id using the sync engine."""
    from sqlalchemy import text
    try:
        with sync_engine.connect() as conn:
            row = conn.execute(
                text("SELECT table_name FROM datasets WHERE id = :id AND status != 'deleted'"),
                {"id": dataset_id},
            ).fetchone()
            return row[0] if row else ""
    except Exception as exc:
        logger.warning("_resolve_table_name failed: %s", exc)
        return ""


def _sse_event(data: dict) -> str:
    """Format a single SSE event line."""
    return f"data: {json.dumps(data, default=str)}\n\n"


async def stream_chat(
    message: str,
    dataset_id: str,
    history: list[dict],
    context: str = "",
    schema_info: dict | None = None,  # kept for backwards-compat; prefer context
) -> AsyncGenerator[str, None]:
    """Async generator that yields SSE events for one chat turn.

    Handles the tool-use loop (up to ``_MAX_TOOL_ROUNDS`` rounds).

    Event shapes:
        {"type": "text_delta", "text": "..."}
        {"type": "tool_executing", "tool": "query_data", "input": {...}}
        {"type": "tool_result", "tool": "query_data", "result": {...}}
        {"type": "scenario_rule", "rule": {...}}
        {"type": "done"}
        {"type": "error", "message": "..."}
    """
    if not settings.ANTHROPIC_API_KEY_CHAT:
        yield _sse_event({"type": "error", "message": "ANTHROPIC_API_KEY_CHAT is not configured"})
        yield _sse_event({"type": "done"})
        return

    client = anthropic.AsyncAnthropic(api_key=settings.ANTHROPIC_API_KEY_CHAT)

    # Build system prompt: prefer rich semantic context; fall back to legacy schema_info
    if context:
        system_prompt = _build_system_prompt_from_context(context)
        table_name = schema_info["table_name"] if schema_info else ""
    elif schema_info:
        system_prompt = build_system_prompt(schema_info)
        table_name = schema_info["table_name"]
    else:
        system_prompt = "You are a data analyst assistant for DataBobIQ."
        table_name = ""

    # Resolve table_name from dataset_id if not provided via schema_info
    if not table_name and dataset_id:
        table_name = _resolve_table_name(dataset_id)

    # Build messages array from history + new user message
    messages: list[dict] = []
    for msg in history:
        role = msg.get("role", "user")
        content = msg.get("content", "")
        if role in ("user", "assistant") and content:
            messages.append({"role": role, "content": content})
    messages.append({"role": "user", "content": message})

    try:
        for _round in range(_MAX_TOOL_ROUNDS + 1):
            # Stream the response
            collected_text = ""
            tool_uses: list[dict] = []

            async with client.messages.stream(
                model=_CHAT_MODEL,
                max_tokens=2048,
                system=system_prompt,
                tools=TOOLS,
                messages=messages,
            ) as stream:
                async for event in stream:
                    if event.type == "content_block_start":
                        if hasattr(event.content_block, "type"):
                            if event.content_block.type == "tool_use":
                                tool_uses.append({
                                    "id": event.content_block.id,
                                    "name": event.content_block.name,
                                    "input": {},
                                })
                    elif event.type == "content_block_delta":
                        if hasattr(event.delta, "text"):
                            collected_text += event.delta.text
                            yield _sse_event({"type": "text_delta", "text": event.delta.text})
                        elif hasattr(event.delta, "partial_json"):
                            # Accumulate tool input JSON incrementally
                            if tool_uses:
                                tool_uses[-1].setdefault("_partial", "")
                                tool_uses[-1]["_partial"] += event.delta.partial_json

            # Parse accumulated tool input JSON
            for tu in tool_uses:
                if "_partial" in tu:
                    try:
                        tu["input"] = json.loads(tu["_partial"])
                    except json.JSONDecodeError:
                        tu["input"] = {}
                    del tu["_partial"]

            # If no tool calls, we're done
            if not tool_uses:
                break

            # Execute tool calls
            # Build the assistant message content (text + tool_use blocks)
            assistant_content: list[dict] = []
            if collected_text:
                assistant_content.append({"type": "text", "text": collected_text})
            for tu in tool_uses:
                assistant_content.append({
                    "type": "tool_use",
                    "id": tu["id"],
                    "name": tu["name"],
                    "input": tu["input"],
                })

            messages.append({"role": "assistant", "content": assistant_content})

            # Execute each tool and build tool_result messages
            tool_results: list[dict] = []
            for tu in tool_uses:
                yield _sse_event({
                    "type": "tool_executing",
                    "tool": tu["name"],
                    "input": tu["input"],
                })

                result = await execute_tool(tu["name"], tu["input"], table_name, dataset_id=dataset_id)

                yield _sse_event({
                    "type": "tool_result",
                    "tool": tu["name"],
                    "result": result,
                })

                # Emit scenario_rule event for create_scenario_rule
                if tu["name"] == "create_scenario_rule" and "error" not in result:
                    yield _sse_event({"type": "scenario_rule", "rule": result})

                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tu["id"],
                    "content": json.dumps(result, default=str),
                })

            messages.append({"role": "user", "content": tool_results})

            # Continue the loop — Claude will see the tool results and respond

    except anthropic.APITimeoutError:
        yield _sse_event({"type": "error", "message": "Claude API timed out — please try again"})
    except anthropic.RateLimitError:
        yield _sse_event({"type": "error", "message": "Rate limited — please wait a moment and try again"})
    except anthropic.APIError as exc:
        logger.error("Anthropic API error in chat: %s", exc)
        yield _sse_event({"type": "error", "message": f"API error: {exc.message}"})
    except Exception as exc:
        logger.error("Unexpected error in stream_chat: %s", exc, exc_info=True)
        yield _sse_event({"type": "error", "message": str(exc)})

    yield _sse_event({"type": "done"})


# ---------------------------------------------------------------------------
# Backwards-compatible non-streaming entry point
# ---------------------------------------------------------------------------

async def chat_with_data(dataset_id: str, message: str, history: list[dict]) -> dict:
    """Non-streaming chat — collects the full response.

    Used by any caller that doesn't need SSE (e.g. tests, internal calls).
    """
    from app.database import AsyncSessionLocal
    from app.models.metadata import Dataset
    from sqlalchemy import select
    from sqlalchemy.orm import selectinload

    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Dataset)
            .where(Dataset.id == dataset_id)
            .options(selectinload(Dataset.columns))
        )
        dataset = result.scalar_one_or_none()
        if dataset is None:
            return {"message": "Dataset not found", "suggested_query": None}

        schema_info = load_schema_info(dataset)

    collected_text = ""
    last_rule = None

    async for event_str in stream_chat(message, dataset_id, history, schema_info=schema_info):
        # Parse the SSE line
        if not event_str.startswith("data: "):
            continue
        try:
            data = json.loads(event_str[6:].strip())
        except json.JSONDecodeError:
            continue

        if data.get("type") == "text_delta":
            collected_text += data.get("text", "")
        elif data.get("type") == "scenario_rule":
            last_rule = data.get("rule")

    return {
        "message": collected_text,
        "suggested_query": None,
        "scenario_rule": last_rule,
    }
