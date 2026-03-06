"""AI-powered schema understanding agent.

Uses ANTHROPIC_API_KEY_AGENT to run a Claude model that corrects heuristic
schema detection results and discovers relationships between tables.

This service is intentionally kept separate from the chat service
(services/chat.py) which uses ANTHROPIC_API_KEY_CHAT.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import anthropic

from app.config import settings

logger = logging.getLogger(__name__)

# Hard timeout for the Claude call so uploads are never blocked indefinitely.
_AGENT_TIMEOUT_SECONDS = 300

_SYSTEM_PROMPT = """\
You are a financial data analyst specializing in ERP and accounting exports.
You receive parsed table schemas from ERP/accounting systems and must:

1. Correct column roles: key, measure, time, attribute, ignore
2. Identify what each table represents (GL entries, chart of accounts,
   company master, cost centers, budget plan, etc.)
3. Suggest fact vs. dimension classification for each table
4. Suggest join relationships between tables (which columns match)
5. Flag data quality issues (high null rates, suspicious value distributions)
6. Suggest cleaner display names for cryptic column headers

You work with German and English financial data. Common patterns:
- Hauptkonto / HK / Konto → main account (role: key)
- Betrag / Amount / Wert → monetary value (role: measure)
- Periode / Period / Monat → time period (role: time)
- Buchungstext / Text / Bezeichnung → description (role: attribute)
- Kostenart / KArt → cost type (role: key, likely FK to dimension)
- Kostenstelle / KST → cost centre (role: key, likely FK to dimension)
- Jahr / Year → fiscal year (role: time)
- Beleg / BelegNr → voucher / document number (role: key)
- Gesellschaft / GesNr → company code (role: key)

Additionally, for each table you must provide:

7. agent_context_notes: A structured summary that will be given to a separate
   Scenario Planning AI agent. Write it as if briefing a new financial analyst:
   - summary: 2-3 sentences describing what this table contains
   - key_dimensions: which columns are the main grouping dimensions and what they represent
   - time_range: what period the data covers (based on sample values)
   - measure_interpretation: what the numeric columns mean (are negatives expenses? is it in EUR/USD?)
   - domain: what domain this data belongs to (financial, sales, hr, etc.)
   - existing_groupings: CRITICAL — look at dimension tables for columns that ALREADY
     provide category/group/hierarchy information. Discover these by their CHARACTERISTICS:
     * Text columns in dimension tables (not the join key column)
     * Low cardinality: typically 3-30 unique values
     * Values are descriptive categories, not numeric codes or IDs
     * The column name can be ANYTHING — you must identify it by the data pattern,
       not by looking for specific names like "reporting_h2" or "account_group"

     For each grouping column you find, you MUST provide:
     - column_name: the ACTUAL column name as it appears in the data
     - source_table: which dimension table it comes from
     - sample_values: the actual values, EXACTLY as they appear (e.g. "Personalaufwand")
     - business_terms: what a user would naturally say for each value (in English
       AND the data language if different). This is the critical bridge — e.g.:
         "Personalaufwand" → ["personnel costs", "Personalaufwand", "HR costs"]
         "Warenaufwand" → ["material costs", "Warenaufwand", "COGS"]
         "Umsatzerlöse" → ["revenue", "Umsatzerlöse", "sales revenue"]

     Include terms in BOTH the data's language AND English if they differ.
     For German data, always include both German and English terms.

   - scenario_hints: write as: "To filter X, use [actual_column_name] = '[actual_value]'.
     To filter Y, use [actual_column_name] = '[actual_value]'."
     NEVER use hardcoded examples from other datasets. Reference the actual column
     and values you discovered in existing_groupings.
     The Scenario Agent should ALWAYS prefer existing grouping columns over raw ID/code columns.

8. value_label_suggestions: IMPORTANT — you only see a sample of values, NOT all values.
   DO NOT try to label every possible value. Instead:

   a) For columns that clearly link to another table (e.g. hauptkonto likely joins
      to a chart of accounts): suggest NO individual labels. The system will auto-populate
      complete labels from the dimension table when the relationship is created.
      Instead, note in agent_context_notes which column likely has a dimension table.

   b) For dimension tables that ALREADY have grouping/category columns: DO NOT suggest
      reclassification transformations that duplicate this. Instead, document the existing
      grouping in agent_context_notes.existing_groupings so the Scenario Agent uses it directly.

   c) For columns with a SMALL number of values (< 20) where all/most values are visible
      in the samples: suggest labels for the values you can see.

   d) Only suggest reclassification transformations when NO existing grouping column covers
      the need. For example: if there's no cost category column in any dimension table,
      THEN suggest creating one from account code patterns.

9. suggested_transformations: ONLY suggest when dimension tables lack a grouping column.
   If reporting_h2 / account_group / kostengruppe already exists: do NOT suggest a
   reclassification — document it in existing_groupings instead.

   When you DO suggest a transformation, provide the full JSON definition:
   - reclassification: must include source_column, output_column, output_type, and rules
     (each rule has a "condition" with op + values/value, and a "result" string;
      last rule has "default" key instead of "condition")
   - calculated_column: must include output_column, output_type, and expression tree
     (each node is {"op": "...", "left": ..., "right": ...} or {"column": "name"} or {"literal": 1.0})
   - concat: must include columns (list), output_column, output_type, separator

   Rules for reclassification conditions:
   - op "between": {"values": ["400000", "499999"]} — works on string representation
   - op "in": {"values": ["A", "B", "C"]}
   - op "equals": {"value": "X"}
   - op "contains": {"value": "substring"}
   - op "starts_with": {"value": "prefix"}

Respond ONLY with a single JSON object — no markdown fences, no extra text.
"""

_RESPONSE_SCHEMA = {
    "type": "object",
    "required": ["tables", "suggested_relationships", "warnings"],
    "properties": {
        "tables": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["name", "description", "table_type", "columns"],
                "properties": {
                    "name": {"type": "string"},
                    "description": {"type": "string"},
                    "table_type": {"type": "string", "enum": ["fact", "dimension", "unknown"]},
                    "columns": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["column_name", "suggested_role"],
                            "properties": {
                                "column_name": {"type": "string"},
                                "suggested_role": {
                                    "type": "string",
                                    "enum": ["key", "measure", "time", "attribute", "ignore"],
                                },
                                "suggested_display_name": {"type": "string"},
                                "reasoning": {"type": "string"},
                            },
                        },
                    },
                    "agent_context_notes": {
                        "type": "object",
                        "properties": {
                            "summary": {"type": "string"},
                            "key_dimensions": {"type": "array", "items": {"type": "string"}},
                            "time_range": {"type": "string"},
                            "measure_interpretation": {"type": "string"},
                            "domain": {"type": "string"},
                            "existing_groupings": {
                                "type": "array",
                                "description": (
                                    "Columns in dimension tables that already group or categorize values. "
                                    "Discover these by their CHARACTERISTICS — not by name. Look for: "
                                    "text columns in dimension tables (not the join key) with low cardinality "
                                    "(3-30 unique values) where values are descriptive categories. "
                                    "The column name can be anything — reporting_h2, account_group, "
                                    "kostengruppe, expense_type, etc. What matters is the PATTERN: "
                                    "it groups rows into human-meaningful categories. "
                                    "ALWAYS prefer documenting these over suggesting reclassification transformations."
                                ),
                                "items": {
                                    "type": "object",
                                    "required": ["column_name", "source_table", "description", "sample_values", "business_terms"],
                                    "properties": {
                                        "column_name": {
                                            "type": "string",
                                            "description": "The actual column name as it appears in the data",
                                        },
                                        "source_table": {
                                            "type": "string",
                                            "description": "Which dimension table this column comes from",
                                        },
                                        "description": {
                                            "type": "string",
                                            "description": "What this grouping represents in plain language",
                                        },
                                        "sample_values": {
                                            "type": "array",
                                            "items": {"type": "string"},
                                            "description": "The actual values in this column, exactly as they appear in the data",
                                        },
                                        "business_terms": {
                                            "type": "array",
                                            "items": {"type": "string"},
                                            "description": (
                                                "CRITICAL: Map each sample_value to a common business term "
                                                "a user would naturally say. Examples: "
                                                "'Personalaufwand' → 'personnel costs'. "
                                                "'Warenaufwand' → 'material costs'. "
                                                "Include terms in BOTH the data language AND English."
                                            ),
                                        },
                                    },
                                },
                            },
                            "scenario_hints": {
                                "type": "string",
                                "description": (
                                    "Instructions for the scenario agent telling it HOW to filter data "
                                    "when users make business-level requests. Reference existing_groupings "
                                    "by their actual column_name and actual values — do NOT use hardcoded "
                                    "examples from other datasets. Write instructions like: "
                                    "'To filter personnel costs, use [actual_column_name] = [actual_value]. "
                                    "To filter revenue, use [actual_column_name] = [actual_value].' "
                                    "ALWAYS tell the agent to use grouping columns over raw ID columns."
                                ),
                            },
                        },
                    },
                    "value_label_suggestions": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["column_name", "labels"],
                            "properties": {
                                "column_name": {"type": "string"},
                                "labels": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "required": ["raw_value", "display_label"],
                                        "properties": {
                                            "raw_value": {"type": "string"},
                                            "display_label": {"type": "string"},
                                            "category": {"type": "string"},
                                        },
                                    },
                                },
                            },
                        },
                    },
                    "suggested_transformations": {
                        "type": "array",
                        "description": (
                            "Only suggest when NO existing grouping column in a dimension table "
                            "already covers the need. If a grouping column already exists, "
                            "do NOT suggest a reclassification — document it in "
                            "existing_groupings instead."
                        ),
                        "items": {
                            "type": "object",
                            "required": ["name", "step_type", "definition"],
                            "properties": {
                                "name": {"type": "string"},
                                "description": {"type": "string"},
                                "step_type": {
                                    "type": "string",
                                    "enum": ["reclassification", "calculated_column", "concat"],
                                },
                                "definition": {"type": "object"},
                                "reason": {"type": "string"},
                            },
                        },
                    },
                },
            },
        },
        "suggested_relationships": {
            "type": "array",
            "items": {
                "type": "object",
                "required": [
                    "source_table", "target_table",
                    "source_column", "target_column",
                    "confidence", "reasoning",
                ],
                "properties": {
                    "source_table": {"type": "string"},
                    "target_table": {"type": "string"},
                    "source_column": {"type": "string"},
                    "target_column": {"type": "string"},
                    "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
                    "reasoning": {"type": "string"},
                },
            },
        },
        "warnings": {"type": "array", "items": {"type": "string"}},
    },
}


def _build_user_message(tables: list[dict]) -> str:
    """Build a rich text description of all tables for the agent prompt."""
    parts: list[str] = []

    for tbl in tables:
        parts.append(f"## Table: {tbl.get('name', 'unnamed')}")
        parts.append(f"- Row count: {tbl.get('row_count', 'unknown')}")
        parts.append(f"- Source file: {tbl.get('filename', 'unknown')}")

        parts.append("\n### Columns")
        parts.append(
            "| column_name | detected_type | column_role | unique_count | null_count | sample_values |"
        )
        parts.append("|---|---|---|---|---|---|")
        for col in tbl.get("columns", []):
            samples = ", ".join(str(v) for v in (col.get("sample_values") or [])[:15])
            parts.append(
                f"| {col.get('column_name','')} "
                f"| {col.get('data_type','')} "
                f"| {col.get('column_role','')} "
                f"| {col.get('unique_count','')} "
                f"| {col.get('null_count','')} "
                f"| {samples} |"
            )

        preview_rows = tbl.get("preview_rows")
        if preview_rows:
            parts.append("\n### First 10 rows (markdown)")
            col_names = [c.get("column_name", "") for c in tbl.get("columns", [])]
            parts.append("| " + " | ".join(col_names) + " |")
            parts.append("|" + "|".join(["---"] * len(col_names)) + "|")
            for row in preview_rows[:10]:
                if isinstance(row, dict):
                    vals = [str(row.get(c, "")) for c in col_names]
                else:
                    vals = [str(v) for v in row]
                parts.append("| " + " | ".join(vals) + " |")

        parts.append("")  # blank line between tables

    return "\n".join(parts)


async def analyze_schema(tables: list[dict]) -> dict:
    """
    Call the Claude agent to analyse table schemas and return structured JSON.

    ``tables`` is a list of dicts, one per parsed sheet/table:
    {
        "name": str,
        "filename": str,
        "row_count": int,
        "columns": [
            {
                "column_name": str,
                "data_type": str,        # heuristic result
                "column_role": str,      # heuristic result
                "unique_count": int,
                "null_count": int,
                "sample_values": list,
            }
        ],
        "preview_rows": list[dict],       # first 10 rows as list of dicts
    }

    Returns the parsed JSON dict from Claude (see _RESPONSE_SCHEMA), or raises
    on timeout / API error so the caller can fall back gracefully.
    """
    if not settings.ANTHROPIC_API_KEY_AGENT:
        raise ValueError("ANTHROPIC_API_KEY_AGENT is not configured")

    client = anthropic.AsyncAnthropic(
        api_key=settings.ANTHROPIC_API_KEY_AGENT,
        timeout=_AGENT_TIMEOUT_SECONDS,
    )

    user_message = _build_user_message(tables)

    response = await client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=8192,
        system=_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_message}],
        # Use tool_use to enforce structured JSON output
        tools=[
            {
                "name": "submit_schema_analysis",
                "description": "Submit the completed schema analysis as structured JSON.",
                "input_schema": _RESPONSE_SCHEMA,
            }
        ],
        tool_choice={"type": "any"},
    )

    # Extract the tool call input
    for block in response.content:
        if block.type == "tool_use" and block.name == "submit_schema_analysis":
            return block.input  # already a dict

    # Fallback: try to parse text content as JSON
    for block in response.content:
        if hasattr(block, "text"):
            try:
                return json.loads(block.text)
            except json.JSONDecodeError:
                pass

    raise ValueError("Agent response contained no usable structured output")


def merge_agent_results(
    heuristic_columns: list[dict],
    agent_table: dict,
) -> tuple[list[dict], dict]:
    """
    Merge agent suggestions into heuristic column list.

    Rules:
    - If agent suggests a role AND heuristic role was the default "attribute",
      replace it with the agent's suggestion.
    - If agent suggests a display name, always use it.
    - Always attach agent reasoning as ai_suggestion on the column.

    Returns (updated_columns, ai_notes_for_dataset).
    """
    agent_col_map: dict[str, dict] = {
        c["column_name"]: c for c in agent_table.get("columns", [])
    }

    updated: list[dict] = []
    for col in heuristic_columns:
        merged = dict(col)
        agent_col = agent_col_map.get(col["column_name"])
        if agent_col:
            # Override role only when heuristic produced the generic default
            if col.get("column_role", "attribute") == "attribute":
                merged["column_role"] = agent_col.get("suggested_role", col.get("column_role"))
            merged["display_name"] = agent_col.get(
                "suggested_display_name", col.get("display_name", col["column_name"])
            )
            merged["ai_suggestion"] = {
                "suggested_role": agent_col.get("suggested_role"),
                "suggested_display_name": agent_col.get("suggested_display_name"),
                "reasoning": agent_col.get("reasoning"),
            }
        updated.append(merged)

    ai_notes = {
        "description": agent_table.get("description", ""),
        "table_type": agent_table.get("table_type", "unknown"),
    }
    return updated, ai_notes
