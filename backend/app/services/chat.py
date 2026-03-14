"""Claude AI chat service with tool use and SSE streaming.

Uses ANTHROPIC_API_KEY_CHAT — completely separate from the schema-agent
(services/schema_agent.py) which uses ANTHROPIC_API_KEY_AGENT.

Architecture
~~~~~~~~~~~~
1.  ``build_system_prompt`` turns the dataset schema into a concise XML
    context block (< 4 000 tokens).
2.  Four tools are exposed to Claude:
    - **query_data** – run a grouped aggregation query
    - **create_scenario_rule** – build a what-if rule + impact preview
    - **list_dimension_values** – look up unique column values
    - **list_scenarios** – list existing scenarios for this dataset
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
_MAX_TOOL_ROUNDS = 5

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
            "comparisons, or to find specific values.  "
            "By default queries the main fact table baseline (with dimension joins).  "
            "Set dataset_name to query a different table in the model."
        ),
        "input_schema": {
            "type": "object",
            "required": ["group_by", "value_column"],
            "properties": {
                "dataset_name": {
                    "type": "string",
                    "description": (
                        "Optional: name of a specific dataset to query instead of the main baseline. "
                        "Use this when you need data from a different table (e.g., a second fact table). "
                        "Must match a dataset name from the data context."
                    ),
                },
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
        "name": "create_scenario_rules",
        "description": (
            "Create one or more what-if scenario rules in a single call. "
            "Can create a new scenario with all rules at once, or add multiple rules to an existing scenario. "
            "ALWAYS submit ALL rules in a single call — do NOT split across multiple calls. "
            "IMPORTANT: For expense/cost categories stored as negative values: "
            "offset rules must NEGATE the amount. 'Increase costs by 300K' → offset: -300000. "
            "'Reduce costs by 300K' → offset: +300000. Multipliers handle sign automatically."
        ),
        "input_schema": {
            "type": "object",
            "required": ["rules"],
            "properties": {
                "scenario_id": {
                    "type": "string",
                    "description": "ID of existing scenario to add rules to. Omit to create new.",
                },
                "scenario_name": {
                    "type": "string",
                    "description": "Name for a new scenario (ignored if scenario_id is set).",
                },
                "base_config": {
                    "type": "object",
                    "description": "Baseline config for new scenarios. Must include base_year.",
                    "properties": {
                        "source": {"type": "string", "enum": ["actuals", "scenario"]},
                        "base_year": {"type": "integer", "description": "REQUIRED baseline year (e.g. 2025)"},
                        "source_scenario_id": {"type": "string"},
                    },
                },
                "rules": {
                    "type": "array",
                    "description": "One or more rules to create. Submit ALL rules in one call.",
                    "items": {
                        "type": "object",
                        "required": ["name", "type"],
                        "properties": {
                            "name": {"type": "string", "description": "Rule name"},
                            "type": {"type": "string", "enum": ["multiplier", "offset"]},
                            "factor": {"type": "number", "description": "For multiplier rules (e.g. 1.05 = +5%)"},
                            "offset": {"type": "number", "description": "For offset rules (total amount). Negative for cost increases."},
                            "distribution": {
                                "type": "string",
                                "enum": ["use_base", "equal"],
                                "description": (
                                    "How to distribute an offset across matching rows. "
                                    "'use_base' = proportional to baseline values (default). "
                                    "'equal' = flat even split across all matching periods. "
                                    "Only relevant for offset rules."
                                ),
                            },
                            "filters": {
                                "type": "object",
                                "description": "Filter to specific rows. Keys are column names, values are arrays.",
                                "additionalProperties": {"type": "array", "items": {}},
                            },
                            "periodFrom": {"type": "string", "description": "Start period YYYY-MM"},
                            "periodTo": {"type": "string", "description": "End period YYYY-MM"},
                        },
                    },
                },
            },
        },
    },
    {
        "name": "list_dimension_values",
        "description": (
            "Look up the unique values for a column (dimension).  "
            "Use this to verify column values before building filters, "
            "or when the user asks 'what categories exist?'  "
            "Set dataset_name to look up values from a specific table."
        ),
        "input_schema": {
            "type": "object",
            "required": ["column_name"],
            "properties": {
                "dataset_name": {
                    "type": "string",
                    "description": "Optional: name of a specific dataset to query.",
                },
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
    {
        "name": "list_knowledge",
        "description": (
            "List knowledge entries saved about this data model. "
            "Returns business term definitions, metric formulas, annotations, "
            "and dataset mappings. Use this when the user asks about business "
            "terms, what the data means, or what's in the knowledge base. "
            "Also use this before creating scenario rules to find the correct "
            "filter columns and values for business terms like 'revenue' or 'COGS'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "entry_type": {
                    "type": "string",
                    "enum": [
                        "term_definition",
                        "interpretation_rule",
                        "metric_definition",
                        "annotation",
                        "dataset_mapping",
                        "relationship",
                        "calculation",
                        "transformation",
                        "definition",
                        "note",
                    ],
                    "description": "Optional: filter to a specific type of knowledge entry.",
                },
                "search": {
                    "type": "string",
                    "description": "Optional: search for entries containing this text.",
                },
            },
        },
    },
    {
        "name": "list_scenarios",
        "description": (
            "List existing scenarios for this dataset. Use this BEFORE creating "
            "a scenario rule to check if a relevant scenario already exists. "
            "If the user says 'also increase X' or 'add another rule', you should "
            "add the rule to the existing scenario rather than creating a new one."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "copy_scenario",
        "description": (
            "Duplicate an existing scenario (copies all rules and base_config) under a new name. "
            "Use when the user says 'copy', 'duplicate', or 'clone' a scenario. "
            "Always call list_scenarios first to get the source scenario ID."
        ),
        "input_schema": {
            "type": "object",
            "required": ["source_scenario_id", "new_name"],
            "properties": {
                "source_scenario_id": {
                    "type": "string",
                    "description": "ID of the scenario to copy",
                },
                "new_name": {
                    "type": "string",
                    "description": "Name for the new copied scenario",
                },
            },
        },
    },
]

# Tools for the Data Understanding Agent (separate persona from scenario agent)
DATA_UNDERSTANDING_TOOLS = [
    {
        "name": "save_knowledge",
        "description": (
            "Save a piece of domain knowledge permanently. The user will see this "
            "in the Knowledge panel and it will be used by all agents in future sessions."
        ),
        "input_schema": {
            "type": "object",
            "required": ["entry_type", "content", "plain_text"],
            "properties": {
                "entry_type": {
                    "type": "string",
                    "enum": ["relationship", "calculation", "transformation", "definition", "note"],
                    "description": (
                        "relationship — cross-table connection. "
                        "calculation — derived metric/formula. "
                        "transformation — data reshaping rule. "
                        "definition — business term or field meaning. "
                        "note — data quirk or exception."
                    ),
                },
                "content": {
                    "type": "object",
                    "description": (
                        "Structured content. Shape depends on entry_type.\n\n"
                        "relationship: {from_table, to_table, description, join_type, "
                        "join_fields: [{from_field, to_field, match_type}], "
                        "join_possible, workaround}\n\n"
                        "calculation: {name, formula_display, result_type, result_unit, "
                        "components: [{id, label, source_table, aggregation, value_column, "
                        "sign, filters: [{column, operator, value}]}], executable: false}\n\n"
                        "transformation: {name, source_table, description, input_grain, "
                        "output_grain, operation, operation_config: {group_by, aggregations, "
                        "filters}, executable: false}\n\n"
                        "definition: {term, aliases, applies_to: {table, column, operator, "
                        "value}, includes_sign_convention, sign_convention}\n\n"
                        "note: {subject, category, description, affects: {tables, columns, "
                        "values}, suggested_action}"
                    ),
                },
                "plain_text": {
                    "type": "string",
                    "description": "One-line human-readable summary",
                },
            },
        },
    },
    {
        "name": "list_knowledge",
        "description": "List existing knowledge entries to check what's already known.",
        "input_schema": {
            "type": "object",
            "properties": {
                "entry_type_filter": {
                    "type": "string",
                    "enum": ["relationship", "calculation", "transformation", "definition", "note"],
                    "description": "Optional: filter by type",
                },
            },
        },
    },
    {
        "name": "query_data",
        "description": (
            "Query the dataset to explore its structure and values. "
            "Use this to understand what data exists before saving knowledge about it. "
            "Set dataset_name to query a specific table in the model."
        ),
        "input_schema": {
            "type": "object",
            "required": ["group_by", "value_column"],
            "properties": {
                "dataset_name": {
                    "type": "string",
                    "description": "Optional: name of a specific dataset to query.",
                },
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
                    "additionalProperties": {"type": "array", "items": {}},
                },
            },
        },
    },
    {
        "name": "list_dimension_values",
        "description": (
            "Look up unique values for a column to understand the data. "
            "Set dataset_name to look up values from a specific table."
        ),
        "input_schema": {
            "type": "object",
            "required": ["column_name"],
            "properties": {
                "dataset_name": {
                    "type": "string",
                    "description": "Optional: name of a specific dataset to query.",
                },
                "column_name": {"type": "string"},
                "search": {"type": "string"},
            },
        },
    },
]

# Intent classifier prompt for routing to the right agent
_INTENT_CLASSIFIER_PROMPT = """Classify the user's message as one of:
- "data_understanding" — about data structure, table relationships, field meanings,
  data quality, calculations, transformations, how the data works
- "scenario" — creating/modifying scenarios, projections, analysis, pivot configuration
- "general" — greeting, help request, general conversation

Examples:
"the invoice table connects to GL through the account field" → data_understanding
"increase revenue by 10% for 2026" → scenario
"what does hauptkonto mean?" → data_understanding
"create a scenario with 5% cost reduction" → scenario
"how is gross margin calculated?" → data_understanding
"show me revenue by month" → scenario
"the period column is YYYY-MM format" → data_understanding
"hi, what can you do?" → general
"exclude company 99 from reports" → data_understanding
"EBITDA is operating profit plus depreciation" → data_understanding

Respond with ONLY the classification word."""


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

def _resolve_dataset(
    tool_input: dict,
    default_table: str,
    default_baseline_df: Any,
    all_table_names: dict[str, str] | None,
    all_baselines: dict[str, Any] | None = None,
) -> tuple[str, Any]:
    """Resolve dataset_name from tool input to (pg_table_name, baseline_df).

    If dataset_name is specified and found in all_table_names, returns that
    table's pg_table_name and its pre-built baseline (if available in
    all_baselines).  Otherwise returns the default fact baseline.
    """
    dataset_name = tool_input.get("dataset_name")
    if not dataset_name or not all_table_names:
        return default_table, default_baseline_df

    pg_table = all_table_names.get(dataset_name)
    if pg_table:
        logger.info("Resolved dataset_name %r to table %s", dataset_name, pg_table)
        resolved_baseline = (all_baselines or {}).get(pg_table)
        return pg_table, resolved_baseline

    logger.warning("dataset_name %r not found in %s, falling back to default",
                   dataset_name, list(all_table_names.keys()))
    return default_table, default_baseline_df


async def execute_tool(
    tool_name: str,
    tool_input: dict,
    table_name: str,
    dataset_id: str = "",
    baseline_df: Any = None,
    all_table_names: dict[str, str] | None = None,
    model_id: str = "",
    all_baselines: dict[str, Any] | None = None,
    all_dataset_ids: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Execute a tool call and return the result dict.

    Runs synchronously against the sync_engine for data queries.
    Returns a dict ready to be JSON-serialised.
    """
    logger.info("Executing tool %s with input %s", tool_name, tool_input)

    if tool_name == "query_data":
        resolved_table, resolved_baseline = _resolve_dataset(
            tool_input, table_name, baseline_df, all_table_names,
            all_baselines=all_baselines,
        )
        return _tool_query_data(tool_input, resolved_table, baseline_df=resolved_baseline)
    elif tool_name == "create_scenario_rules":
        return _tool_create_scenario_rules(tool_input, table_name, baseline_df=baseline_df)
    elif tool_name == "create_scenario_rule":
        # Backward compat: wrap singular call into array format
        wrapped = dict(tool_input)
        if "rules" not in wrapped:
            wrapped["rules"] = [{k: v for k, v in tool_input.items()
                                 if k not in ("scenario_id", "scenario_name", "base_config")}]
        return _tool_create_scenario_rules(wrapped, table_name, baseline_df=baseline_df)
    elif tool_name == "list_dimension_values":
        resolved_table, resolved_baseline = _resolve_dataset(
            tool_input, table_name, baseline_df, all_table_names,
            all_baselines=all_baselines,
        )
        resolved_ds_id = dataset_id
        if resolved_table != table_name and all_dataset_ids:
            resolved_ds_id = all_dataset_ids.get(resolved_table, dataset_id)
        return _tool_list_dimension_values(
            tool_input, resolved_table, dataset_id=resolved_ds_id, baseline_df=resolved_baseline
        )
    elif tool_name == "list_scenarios":
        return _tool_list_scenarios(dataset_id)
    elif tool_name == "copy_scenario":
        return _tool_copy_scenario(tool_input)
    elif tool_name == "save_knowledge":
        return _tool_save_knowledge(tool_input, dataset_id)
    elif tool_name == "list_knowledge":
        return _tool_list_knowledge(tool_input, dataset_id=dataset_id, model_id=model_id)
    else:
        return {"error": f"Unknown tool: {tool_name}"}


def _tool_query_data(inp: dict, table_name: str, baseline_df: Any = None) -> dict:
    """Run a grouped aggregation query and return ≤ 50 rows."""
    import polars as pl

    group_by: list[str] = inp.get("group_by", [])
    value_column: str = inp["value_column"]
    agg: str = inp.get("aggregation", "sum")
    filters: dict = inp.get("filters") or {}

    # Use the enriched baseline (with dimension columns) when available
    if baseline_df is not None:
        df = baseline_df
    else:
        try:
            df = storage_svc.read_dataset(sync_engine, table_name)
        except Exception as e:
            logger.error("Failed to read table %s: %s", table_name, e)
            return {"error": f"Could not read table '{table_name}': {e}"}

    if df is None or len(df) == 0:
        return {"error": f"Table '{table_name}' is empty or not found"}

    # Apply filters
    for col_name, values in filters.items():
        if col_name in df.columns and isinstance(values, list):
            str_vals = [str(v) for v in values]
            df = df.filter(pl.col(col_name).cast(pl.Utf8).is_in(str_vals))

    if value_column not in df.columns:
        return {"error": f"Column '{value_column}' not found in dataset"}

    # Guard: ensure value_column is numeric before attempting aggregation
    col_dtype = df[value_column].dtype
    if col_dtype not in (pl.Float32, pl.Float64, pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64, pl.Decimal):
        numeric_cols = [
            c for c in df.columns
            if df[c].dtype in (pl.Float32, pl.Float64, pl.Int32, pl.Int64)
            and c != "_row_id"
        ]
        hint = f" Available numeric columns: {numeric_cols}" if numeric_cols else ""
        return {"error": f"Column '{value_column}' is not numeric (type: {col_dtype}). Choose a measure/numeric column for aggregation.{hint}"}

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


def _tool_create_scenario_rules(inp: dict, table_name: str, baseline_df: Any = None) -> dict:
    """Build multiple scenario rules and compute an impact preview for each."""
    import polars as pl

    rules_input = inp.get("rules", [])
    if not rules_input:
        return {"error": "No rules provided"}

    if baseline_df is not None:
        df = baseline_df
    else:
        df = storage_svc.read_dataset(sync_engine, table_name)

    built_rules = []
    total_delta = 0.0

    for rule_inp in rules_input:
        rule: dict[str, Any] = {
            "id": None,
            "name": rule_inp.get("name", "Unnamed rule"),
            "type": rule_inp.get("type", "multiplier"),
            "filters": rule_inp.get("filters") or {},
            "distribution": rule_inp.get("distribution", "use_base"),
        }
        if rule["type"] == "multiplier":
            rule["factor"] = float(rule_inp.get("factor", 1.0))
        else:
            rule["offset"] = float(rule_inp.get("offset", 0.0))
        if rule_inp.get("periodFrom"):
            rule["periodFrom"] = rule_inp["periodFrom"]
        if rule_inp.get("periodTo"):
            rule["periodTo"] = rule_inp["periodTo"]

        try:
            mask = pl.lit(True)
            for col_name, values in rule.get("filters", {}).items():
                if col_name in df.columns and isinstance(values, list):
                    str_vals = [str(v) for v in values]
                    mask = mask & pl.col(col_name).cast(pl.Utf8).is_in(str_vals)

            matched = df.filter(mask)
            affected = len(matched)
            total_rows = len(df)

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
                    estimated_delta = current_sum * (rule.get("factor", 1.0) - 1)
                else:
                    estimated_delta = rule.get("offset", 0.0)

            preview: dict[str, Any] = {
                "affected_rows": affected,
                "total_rows": total_rows,
                "estimated_delta": round(estimated_delta, 2) if estimated_delta is not None else None,
            }
            if total_rows > 0 and affected == total_rows and rule.get("filters"):
                preview["warning"] = f"Filter matched ALL {total_rows} rows — values may not exist in data"
            elif affected == 0:
                preview["warning"] = "No rows matched — rule will have no effect"
            elif total_rows > 0 and affected == total_rows and not rule.get("filters"):
                preview["warning"] = f"No filter — rule applies to ALL {total_rows} rows"
            rule["_preview"] = preview
            if estimated_delta:
                total_delta += estimated_delta
        except Exception as exc:
            logger.warning("Impact preview failed for rule %r: %s", rule.get("name"), exc)
            rule["_preview"] = {"error": str(exc)}

        built_rules.append(rule)

    return {
        "rules": built_rules,
        "rule_count": len(built_rules),
        "total_estimated_delta": round(total_delta, 2),
        "base_config": inp.get("base_config"),
        "scenario_id": inp.get("scenario_id"),
        "scenario_name": inp.get("scenario_name"),
    }


def _tool_list_dimension_values(
    inp: dict, table_name: str, dataset_id: str = "", baseline_df: Any = None
) -> dict:
    """Return up to 100 unique values for a column, with semantic labels when available."""
    import polars as pl
    from sqlalchemy import text

    col_name: str = inp["column_name"]
    search: str | None = inp.get("search")

    # Use baseline (with dimension columns) when the column exists there
    if baseline_df is not None and col_name in baseline_df.columns:
        df = baseline_df.select([col_name])
    else:
        try:
            df = storage_svc.read_dataset(sync_engine, table_name, columns=[col_name])
        except Exception as e:
            logger.error("Failed to read column %s from %s: %s", col_name, table_name, e)
            return {"error": f"Could not read column '{col_name}' from table '{table_name}': {e}"}

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


def _tool_list_scenarios(dataset_id: str) -> dict:
    """Return existing scenarios for the current dataset."""
    from sqlalchemy import text
    try:
        with sync_engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT id, name, rules, color, base_config
                    FROM scenarios
                    WHERE dataset_id = :ds_id
                    ORDER BY created_at DESC
                """),
                {"ds_id": dataset_id},
            ).fetchall()
            scenarios = []
            for row in rows:
                rules_data = row[2] if isinstance(row[2], list) else []
                scenarios.append({
                    "id": row[0],
                    "name": row[1],
                    "rule_count": len(rules_data),
                    "rules_summary": [
                        {
                            "name": r.get("name", ""),
                            "type": r.get("type", ""),
                            "factor": r.get("factor"),
                            "offset": r.get("offset"),
                            "distribution": r.get("distribution", "use_base"),
                            "filters": r.get("filters", {}),
                            "periodFrom": r.get("periodFrom"),
                            "periodTo": r.get("periodTo"),
                        }
                        for r in rules_data
                    ],
                    "color": row[3],
                    "base_config": row[4] if row[4] else None,
                })
            return {"scenarios": scenarios, "count": len(scenarios)}
    except Exception as exc:
        return {"error": str(exc)}


def _tool_save_knowledge(inp: dict, dataset_id: str) -> dict:
    """Insert a knowledge entry for the dataset."""
    import uuid as _uuid
    from sqlalchemy import text

    entry_type = inp.get("entry_type", "note")
    plain_text = inp.get("plain_text", "")
    content = inp.get("content") or {}
    # Default to "suggested" so user can confirm in the Knowledge panel
    confidence = inp.get("confidence", "suggested")

    if not plain_text:
        return {"error": "plain_text is required"}

    try:
        new_id = _uuid.uuid4().hex
        with sync_engine.begin() as conn:
            # Look up model_id from the dataset
            ds_row = conn.execute(
                text("SELECT model_id FROM datasets WHERE id = :ds_id"),
                {"ds_id": dataset_id},
            ).fetchone()
            model_id = ds_row[0] if ds_row else None

            conn.execute(
                text("""
                    INSERT INTO knowledge_entries
                      (id, model_id, dataset_id, entry_type, plain_text, content, confidence, source)
                    VALUES (:id, :model_id, :dataset_id, :entry_type, :plain_text, :content, :confidence, 'chat_agent')
                """),
                {
                    "id": new_id,
                    "model_id": model_id,
                    "dataset_id": dataset_id,
                    "entry_type": entry_type,
                    "plain_text": plain_text,
                    "content": json.dumps(content),
                    "confidence": confidence,
                },
            )
        return {"id": new_id, "entry_type": entry_type, "plain_text": plain_text, "saved": True}
    except Exception as exc:
        logger.warning("Failed to save knowledge entry: %s", exc)
        return {"error": str(exc)}


def _tool_list_knowledge(inp: dict, dataset_id: str = "", model_id: str = "") -> dict:
    """Return knowledge entries for the model/dataset (excludes rejected entries)."""
    from sqlalchemy import text

    entry_type_filter = inp.get("entry_type_filter") or inp.get("entry_type")
    search = inp.get("search", "")
    try:
        with sync_engine.connect() as conn:
            params: dict = {}
            where_parts = ["(confidence IS NULL OR confidence != 'rejected')"]

            # Prefer model_id scope; fall back to dataset_id
            if model_id:
                where_parts.append("(model_id = :model_id OR model_id IS NULL)")
                params["model_id"] = model_id
            elif dataset_id:
                where_parts.append("(dataset_id = :ds_id OR dataset_id IS NULL)")
                params["ds_id"] = dataset_id

            if entry_type_filter:
                where_parts.append("entry_type = :etype")
                params["etype"] = entry_type_filter

            where = " AND ".join(where_parts)
            rows = conn.execute(
                text(f"""
                    SELECT id, entry_type, plain_text, confidence, source
                    FROM knowledge_entries
                    WHERE {where}
                    ORDER BY entry_type, created_at DESC
                """),
                params,
            ).fetchall()

        results = [
            {"id": r[0], "type": r[1], "summary": r[2], "confidence": r[3], "source": r[4]}
            for r in rows
            if not search or search.lower() in (r[2] or "").lower()
        ]
        return {
            "entries": results,
            "count": len(results),
        }
    except Exception as exc:
        logger.warning("Failed to list knowledge entries: %s", exc)
        return {"error": str(exc)}


def _tool_copy_scenario(inp: dict) -> dict:
    """Duplicate an existing scenario under a new name."""
    import uuid as _uuid
    from sqlalchemy import text

    source_id = inp.get("source_scenario_id", "")
    new_name = inp.get("new_name", "Copy")
    try:
        with sync_engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM scenarios WHERE id = :id"),
                {"id": source_id},
            ).fetchone()
            if not row:
                return {"error": f"Scenario {source_id!r} not found"}
            new_id = _uuid.uuid4().hex
            conn.execute(
                text("""
                    INSERT INTO scenarios (id, name, dataset_id, rules, color, base_config, created_at, updated_at)
                    SELECT :new_id, :new_name, dataset_id, rules, color, base_config, NOW(), NOW()
                    FROM scenarios WHERE id = :source_id
                """),
                {"new_id": new_id, "new_name": new_name, "source_id": source_id},
            )
            conn.commit()
            return {"id": new_id, "name": new_name, "copied_from": source_id}
    except Exception as exc:
        return {"error": str(exc)}


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
        "- KNOWLEDGE BASE: The data context includes a <knowledge> section with business\n"
        "  definitions, metrics, and notes saved by the Data Understanding Agent or the user.\n"
        "  ALWAYS check the <knowledge> section before answering questions about business terms,\n"
        "  metrics, or data relationships. If the user asks for a metric with a <calculation>\n"
        "  entry, use the defined formula and filters. If they ask about cross-table analysis\n"
        "  and there's a <relationship>, explain the relationship and any limitations.\n"
        "  You can also call list_knowledge to explicitly retrieve all saved entries.\n"
        "  When the user asks 'what do you know about this data?' or 'what's in the knowledge\n"
        "  base?', call list_knowledge and summarize the results.\n"
        "- CRITICAL: When the user asks about cost categories, expense types, revenue, or any\n"
        "  business concept:\n"
        "  1. FIRST check <existing_groupings> — these are pre-existing columns from dimension\n"
        "     tables that already categorize the data. The column could be named ANYTHING.\n"
        "     Match the user's request to the <business_terms> in each grouping entry.\n"
        "  2. THEN check <glossary> for direct term-to-filter mappings.\n"
        "  3. THEN check <values> labels on individual columns.\n"
        "  4. Only use list_dimension_values as a last resort.\n"
        "- When the user says business terms like 'personnel costs', 'material costs', 'revenue':\n"
        "  → Find the grouping column whose <business_terms> include that concept.\n"
        "  → Use the corresponding <values> entries to build your filter with actual data values.\n"
        "  → NEVER filter on numeric ID/code columns when a grouping column exists.\n"
        "- When building scenario rules, ALWAYS use grouping/category columns for filters\n"
        "  when available, not numeric ID columns. The rule should be readable by a human.\n"
        "- When the user asks about their data, use query_data with the correct column names.\n"
        "- MULTI-TABLE QUERIES: When the data context shows multiple datasets (tables), you can\n"
        "  query any of them by setting dataset_name in query_data or list_dimension_values.\n"
        "  If the user asks a question that spans two tables (e.g., 'cost per billable hour'\n"
        "  needing GL costs + Tempo hours), query each table separately and combine the results\n"
        "  in your reasoning. Do NOT try to join the tables — query each one and do the math.\n"
        "  Example: total_cost = query GL filtered to personnel, total_hours = query Tempo,\n"
        "  cost_per_hour = total_cost / total_hours.\n"
        "- When the user asks 'what if', create a scenario rule with create_scenario_rule.\n"
        "- When the user asks to project or forecast future periods (e.g. 'what if 2026 revenue\n"
        "  increases by 10%'), create an offset or multiplier rule with periodFrom/periodTo set\n"
        "  to the target year (e.g. periodFrom='2026-01', periodTo='2026-12').\n"
        "  The scenario's base_year provides the historical template; rules define the changes.\n"
        "- If the data context includes scenario_hints, follow those instructions exactly.\n"
        "- FINANCIAL RULES: When building scenario rules for financial data:\n"
        "  * ALWAYS include a filter — NEVER apply a rule to all rows.\n"
        "  * 'Increase revenue by 10%' → filter on revenue category ONLY.\n"
        "  * 'Reduce costs by 5%' → filter on cost categories ONLY, not revenue.\n"
        "  * If unsure which column/value to use, call list_dimension_values first.\n"
        "  * If there's no clear grouping column, ASK the user to specify.\n"
        "  * After creating a rule, check _preview warnings in the tool result and explain to the\n"
        "    user if the filter matched all rows or zero rows — that indicates something is wrong.\n"
        "- RULE CREATION: ALWAYS submit ALL rules in a single create_scenario_rules call.\n"
        "  Do NOT create rules one at a time across multiple calls.\n"
        "  Put all rules in the 'rules' array of one create_scenario_rules call.\n"
        "- RULE DISTRIBUTION: Each offset rule has a distribution mode:\n"
        "  * 'use_base' (proportional, DEFAULT): distributes the offset weighted by each row's\n"
        "    share of the total absolute baseline. Months with larger amounts get proportionally more.\n"
        "    Example: 300K on revenue → Jan (30%% of annual) gets 90K, Feb (8%%) gets 24K.\n"
        "  * 'equal': splits the offset evenly across all matching periods.\n"
        "    Example: 300K across 12 months → each month gets exactly 25K.\n"
        "  * If the user doesn't specify, use 'use_base' by default.\n"
        "  * Multiplier rules apply uniformly — distribution only matters for offset rules.\n"
        "- SCENARIO MANAGEMENT:\n"
        "  * Before creating a new scenario, call list_scenarios to check what already exists.\n"
        "  * If the user says 'also increase X', 'add another rule', or similar → add the rule to\n"
        "    the EXISTING scenario by passing scenario_id to create_scenario_rules.\n"
        "  * Only create a brand-new scenario when the user explicitly asks for one.\n"
        "  * Every scenario MUST have a base_year in its base_config. When creating a new scenario,\n"
        "    ALWAYS include base_config with base_year set to the year the user wants as baseline.\n"
        "    If the user hasn't specified a year, ask: 'Which year should I use as the baseline?'\n"
        "- COST/EXPENSE SIGN CONVENTION: In this data, expenses and costs are stored as\n"
        "  NEGATIVE values. Revenue is POSITIVE. This means:\n"
        "  * 'Increase revenue by 10%' → multiplier 1.10 ✓\n"
        "  * 'Increase costs by 10%' → multiplier 1.10 (1.10 × negative = more negative) ✓\n"
        "  * 'Add 300K to revenue' → offset +300000 ✓\n"
        "  * 'Increase costs by 300K' → offset -300000 (costs are negative, making them more\n"
        "    negative = subtract) ✓\n"
        "  * 'Reduce costs by 300K' → offset +300000 (reducing negative cost = adding) ✓\n"
        "  * RULE: For offset rules on expense/cost categories, NEGATE the offset amount.\n"
        "  * For multiplier rules, sign is handled automatically.\n"
        "- Format numbers with thousand separators.\n"
        "- If the data appears to be German, respond in German when the user writes in German.\n"
        "</instructions>"
    )


async def _classify_intent(message: str, client: anthropic.AsyncAnthropic) -> str:
    """Classify user message intent: 'scenario', 'data_understanding', or 'general'.

    Falls back to 'scenario' if classification fails.
    """
    if message.strip() == "__ONBOARDING_START__":
        return "data_understanding"

    try:
        resp = await client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=20,
            system=_INTENT_CLASSIFIER_PROMPT,
            messages=[{"role": "user", "content": message}],
        )
        intent = resp.content[0].text.strip().lower().replace('"', "").split()[0]
        if intent in ("data_understanding", "scenario", "general"):
            return intent
        return "scenario"
    except Exception:
        return "scenario"


def _build_data_understanding_prompt(context: str) -> str:
    """System prompt for the Data Understanding Agent."""
    return (
        "You are the Data Understanding Agent for dataBobIQ, a financial analytics platform.\n"
        "Your role is to help the user document and clarify their data structure.\n\n"
        f"{context}\n\n"
        "<tools>\n"
        "- save_knowledge: Save a knowledge entry permanently (relationship, calculation,\n"
        "  transformation, definition, or note)\n"
        "- list_knowledge: Check what's already been saved\n"
        "- query_data: Query a dataset. Set dataset_name to query a specific table.\n"
        "- list_dimension_values: Look up unique column values. Set dataset_name for a specific table.\n"
        "</tools>\n"
        "<multi_table>\n"
        "- MULTI-TABLE QUERIES: When the data context shows multiple datasets (tables), you can\n"
        "  query any of them by setting dataset_name in query_data or list_dimension_values.\n"
        "  If exploration requires data from multiple tables, query each one separately and\n"
        "  combine the results in your reasoning. Do NOT try to join tables.\n"
        "</multi_table>\n"
        "<behavior>\n"
        "SAVING KNOWLEDGE:\n"
        "When the user explains something about their data, save it using save_knowledge\n"
        "with the correct type and a well-structured content object.\n"
        "For calculations and transformations, make the content PRECISE and COMPLETE:\n"
        "- Always include specific column names, table names, filter values\n"
        "- Use the standard operator set: eq, neq, gt, lt, gte, lte, in, not_in, contains, between\n"
        "- Define every component of a formula separately with its filters and aggregation\n"
        "- Set 'executable': false (Level 2 is not yet active)\n"
        "After saving, confirm briefly: 'Saved — this is now in your Knowledge panel.'\n\n"
        "ASKING FOLLOW-UPS:\n"
        "When an explanation is incomplete, ask 1-2 focused follow-up questions:\n"
        "- 'Which column identifies COGS? Is it reporting_h2 = Warenaufwand?'\n"
        "- 'Is that a direct join on customer_id, or is there a mapping table?'\n"
        "Keep it conversational, not interrogative.\n\n"
        "CONFIRMING COMPLEX ENTRIES:\n"
        "For multi-component calculations or non-obvious relationships, summarize your\n"
        "understanding before saving.\n\n"
        "QUALITY STANDARD:\n"
        "- Bad: 'Invoices and GL are related' (too vague)\n"
        "- Good: 'Invoice lines create GL postings in accounts 500000-599999. No direct FK.\n"
        "  Both tables share customer_id (invoice) ↔ debitor_id (GL) and the same period format.'\n"
        "</behavior>"
    )


def _build_onboarding_prompt(context: str) -> str:
    """System prompt for the onboarding flow — proactive exploration of a new dataset."""
    return (
        "You are the Data Understanding Agent for dataBobIQ.\n"
        "The user just uploaded data. Analyze what the system discovered and ask about what's MISSING.\n\n"
        f"{context}\n\n"
        "<behavior>\n"
        "1. Brief intro: 'I've looked through your data. Here's what I see, and a few things I'd like\n"
        "   to understand better.'\n"
        "2. Summarize key findings: tables, row counts, key columns, detected relationships.\n"
        "3. Ask 3-5 SPECIFIC questions. Priorities:\n"
        "   - Multiple tables with no relationships → how do they connect?\n"
        "   - Financial data → key calculations needed? (margins, EBITDA, etc.)\n"
        "   - Codes/IDs with no labels → what do they mean?\n"
        "   - Time columns → reporting grain, any transformation needs?\n"
        "4. Offer your best guess and let the user confirm:\n"
        "   'It looks like hauptkonto groups into reporting_h2. Is that the hierarchy you use?'\n"
        "5. Number your questions so the user can reply to specific ones.\n"
        "6. After the user responds, save each answer using save_knowledge.\n"
        "</behavior>"
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
    baseline_df: Any = None,  # pl.DataFrame | None — enriched fact+dim join
    agent_mode: str = "scenario",  # "data_understanding" | "scenario" — set by frontend tab
    all_table_names: dict[str, str] | None = None,  # dataset_name → pg_table_name mapping
    model_id: str = "",
    all_baselines: dict[str, Any] | None = None,  # pg_table_name → baseline_df for all fact tables
    all_dataset_ids: dict[str, str] | None = None,  # pg_table_name → dataset_id for all datasets
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

    # Route to the right agent persona based on the active frontend tab
    # (agent_mode is set explicitly by the frontend — no intent classifier needed)
    is_onboarding = message.strip() == "__ONBOARDING_START__"
    agent_type = "data_understanding" if (is_onboarding or agent_mode == "data_understanding") else "scenario"

    # Build system prompt: prefer rich semantic context; fall back to legacy schema_info
    if context:
        if is_onboarding:
            system_prompt = _build_onboarding_prompt(context)
        elif agent_type == "data_understanding":
            system_prompt = _build_data_understanding_prompt(context)
        else:
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

    # Select tools based on agent type
    active_tools = DATA_UNDERSTANDING_TOOLS if agent_type == "data_understanding" else TOOLS

    # Build messages array from history + new user message
    messages: list[dict] = []
    for msg in history:
        role = msg.get("role", "user")
        content_val = msg.get("content", "")
        if role in ("user", "assistant") and content_val:
            messages.append({"role": role, "content": content_val})

    # Onboarding: synthesize the effective user turn; hide the __ONBOARDING_START__ trigger
    if is_onboarding:
        messages.append({
            "role": "user",
            "content": (
                "The user just uploaded their data. Analyze the data context and "
                "introduce yourself briefly. Then ask 3-5 specific questions about "
                "things you couldn't determine automatically. Focus on: "
                "1) Relationships between tables, "
                "2) Key calculations or derived metrics needed, "
                "3) Any data transformations or business rules. "
                "Keep it conversational. Number your questions."
            ),
        })
    else:
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
                tools=active_tools,
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
                            yield _sse_event({"type": "text_delta", "text": event.delta.text, "agent": agent_type})
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

                result = await execute_tool(
                    tu["name"], tu["input"], table_name,
                    dataset_id=dataset_id, baseline_df=baseline_df,
                    all_table_names=all_table_names,
                    model_id=model_id,
                    all_baselines=all_baselines,
                    all_dataset_ids=all_dataset_ids,
                )

                yield _sse_event({
                    "type": "tool_result",
                    "tool": tu["name"],
                    "result": result,
                })

                # Emit scenario_rules event for create_scenario_rules (and backward compat singular)
                if tu["name"] in ("create_scenario_rules", "create_scenario_rule") and "error" not in result:
                    yield _sse_event({
                        "type": "scenario_rules",
                        "rules": result.get("rules", []),
                        "scenario_id": result.get("scenario_id"),
                        "scenario_name": result.get("scenario_name"),
                        "base_config": result.get("base_config"),
                    })

                # Emit scenario_copied event for copy_scenario
                if tu["name"] == "copy_scenario" and "error" not in result:
                    yield _sse_event({
                        "type": "scenario_copied",
                        "id": result.get("id"),
                        "name": result.get("name"),
                        "copied_from": result.get("copied_from"),
                    })

                # Emit knowledge_saved event when knowledge is saved successfully
                if tu["name"] == "save_knowledge" and result.get("saved"):
                    yield _sse_event({
                        "type": "knowledge_saved",
                        "id": result.get("id"),
                        "entry_type": result.get("entry_type"),
                        "plain_text": result.get("plain_text"),
                        "dataset_id": dataset_id,
                    })

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
