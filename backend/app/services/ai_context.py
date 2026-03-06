"""AI Context Builder — generates rich, token-efficient context for AI agents.

Reads from the semantic layer (descriptions, synonyms, value labels) and
dataset metadata to produce structured XML context blocks that make AI agents
genuinely understand the data.

Used by:
- routes.py chat endpoint (passed to stream_chat as `context`)
- schema_agent.py (re-analysis with prior context, future use)
"""

from __future__ import annotations

import logging
import math
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.database import sync_engine
from app.models.metadata import Dataset, DatasetRelationship, SemanticColumn
from app.services import storage as storage_svc

logger = logging.getLogger(__name__)

_MAX_LABELS_INLINE = 20  # show labels inline up to this count, hint for more beyond


async def build_agent_context(
    dataset_ids: list[str],
    db: AsyncSession,
    max_tokens: int = 4000,
    include_labels: bool = True,
    include_stats: bool = True,
) -> str:
    """Build a structured XML context block for AI agent consumption.

    Includes:
    1. Dataset descriptions (from agent_context_notes)
    2. Column descriptions with synonyms (from semantic_columns)
    3. Value labels for key/attribute columns (from semantic_value_labels)
    4. Measure statistics: min, max, sum (computed on demand)
    5. Relationships between datasets
    6. Business glossary derived from scenario_hints
    """
    import polars as pl

    if not dataset_ids:
        return ""

    # Load all requested datasets with columns and semantic data
    ds_result = await db.execute(
        select(Dataset)
        .where(Dataset.id.in_(dataset_ids), Dataset.status != "deleted")
        .options(selectinload(Dataset.columns))
    )
    datasets = ds_result.scalars().all()
    if not datasets:
        return ""

    # Load all semantic columns (with labels) for these datasets in one query
    sc_result = await db.execute(
        select(SemanticColumn)
        .where(SemanticColumn.dataset_id.in_(dataset_ids))
        .options(selectinload(SemanticColumn.labels))
    )
    all_sem_cols = sc_result.scalars().all()
    # Index: dataset_id → {column_name → SemanticColumn}
    sem_by_ds: dict[str, dict[str, SemanticColumn]] = {}
    for sc in all_sem_cols:
        sem_by_ds.setdefault(sc.dataset_id, {})[sc.column_name] = sc

    # Load relationships between these datasets
    rel_result = await db.execute(
        select(DatasetRelationship).where(
            DatasetRelationship.source_dataset_id.in_(dataset_ids) |
            DatasetRelationship.target_dataset_id.in_(dataset_ids)
        )
    )
    all_rels = rel_result.scalars().all()

    parts: list[str] = ["<data_context>"]

    for ds in datasets:
        notes = ds.agent_context_notes or {}
        sem_cols_for_ds = sem_by_ds.get(ds.id, {})

        parts.append(f'  <dataset name="{_esc(ds.name)}" rows="{ds.row_count}">')

        if notes.get("summary"):
            parts.append(f"    <description>{_esc(notes['summary'])}</description>")

        if notes.get("scenario_hints"):
            parts.append(f"    <scenario_hints>{_esc(notes['scenario_hints'])}</scenario_hints>")

        if notes.get("existing_groupings"):
            parts.append(f"    <existing_groupings>{_esc(notes['existing_groupings'])}</existing_groupings>")

        if notes.get("measure_interpretation"):
            parts.append(f"    <measure_interpretation>{_esc(notes['measure_interpretation'])}</measure_interpretation>")

        # Load measure stats once
        measure_cols = [c for c in ds.columns if c.column_role == "measure"]
        measure_stats: dict[str, dict] = {}
        if include_stats and measure_cols:
            try:
                col_names = [c.column_name for c in measure_cols]
                df = storage_svc.read_dataset(sync_engine, ds.table_name, columns=col_names)
                for col in measure_cols:
                    if col.column_name in df.columns:
                        series = df[col.column_name].cast(pl.Float64, strict=False).drop_nulls()
                        if len(series) > 0:
                            measure_stats[col.column_name] = {
                                "min": round(float(series.min()), 2),
                                "max": round(float(series.max()), 2),
                                "sum": round(float(series.sum()), 2),
                            }
            except Exception as exc:
                logger.warning("Failed to compute measure stats for %s: %s", ds.name, exc)

        # Columns block
        parts.append("    <columns>")
        for col in ds.columns:
            if col.column_role == "ignore":
                continue
            sem = sem_cols_for_ds.get(col.column_name)
            attrs = (
                f'name="{_esc(col.column_name)}" '
                f'display="{_esc(col.display_name)}" '
                f'role="{col.column_role}" '
                f'type="{col.data_type}"'
            )
            parts.append(f"      <column {attrs}>")

            # Description (from semantic layer or AI suggestion)
            desc = (sem.description if sem else None) or _get_ai_reasoning(col)
            if desc:
                parts.append(f"        <description>{_esc(desc)}</description>")

            # Synonyms
            if sem and sem.synonyms:
                syns = ", ".join(_esc(s) for s in sem.synonyms)
                parts.append(f"        <synonyms>{syns}</synonyms>")

            if col.column_role == "measure":
                stats = measure_stats.get(col.column_name)
                if stats:
                    parts.append(
                        f'        <stats min="{stats["min"]}" max="{stats["max"]}" sum="{stats["sum"]}"/>'
                    )
            elif col.column_role in ("key", "attribute", "time") and include_labels:
                # Value labels from semantic layer
                if sem and sem.labels:
                    total = len(sem.labels)
                    shown = sem.labels[:_MAX_LABELS_INLINE]
                    parts.append(f'        <values count="{total}">')
                    for lbl in shown:
                        cat_attr = f' category="{_esc(lbl.category)}"' if lbl.category else ""
                        parts.append(
                            f'          <value raw="{_esc(lbl.raw_value)}" '
                            f'label="{_esc(lbl.display_label)}"{cat_attr}/>'
                        )
                    if total > _MAX_LABELS_INLINE:
                        parts.append(
                            f'          <!-- +{total - _MAX_LABELS_INLINE} more — '
                            f'use list_dimension_values to search -->'
                        )
                    parts.append("        </values>")
                elif col.sample_values:
                    # Fall back to raw sample values
                    top = col.sample_values[:15]
                    vals_str = ", ".join(_esc(str(v)) for v in top)
                    unique = col.unique_count or "?"
                    parts.append(f'        <sample_values unique="{unique}">{vals_str}</sample_values>')

            parts.append("      </column>")
        parts.append("    </columns>")

        # Relationships for this dataset
        ds_rels = [
            r for r in all_rels
            if r.source_dataset_id == ds.id or r.target_dataset_id == ds.id
        ]
        if ds_rels:
            parts.append("    <relationships>")
            for r in ds_rels:
                if r.source_dataset_id == ds.id:
                    other_ds = next((d for d in datasets if d.id == r.target_dataset_id), None)
                    other_name = other_ds.name if other_ds else r.target_dataset_id
                    cov = f"{r.coverage_pct}%" if r.coverage_pct is not None else "?"
                    parts.append(
                        f'      <rel from="{_esc(r.source_column)}" '
                        f'to="{_esc(other_name)}.{_esc(r.target_column)}" coverage="{cov}"/>'
                    )
                else:
                    other_ds = next((d for d in datasets if d.id == r.source_dataset_id), None)
                    other_name = other_ds.name if other_ds else r.source_dataset_id
                    cov = f"{r.coverage_pct}%" if r.coverage_pct is not None else "?"
                    parts.append(
                        f'      <rel from="{_esc(r.target_column)}" '
                        f'to="{_esc(other_name)}.{_esc(r.source_column)}" coverage="{cov}"/>'
                    )
            parts.append("    </relationships>")

        parts.append("  </dataset>")

    # Glossary: extract term→filter mappings from scenario_hints across all datasets
    glossary_terms: list[tuple[str, str]] = []
    for ds in datasets:
        hints = (ds.agent_context_notes or {}).get("scenario_hints", "")
        if hints:
            # Simple heuristic: find "X = 'Y'" patterns in hints
            import re
            for m in re.finditer(r"(\w[\w\s]+?)\s+(?:is|=)\s+'([^']+)'", hints):
                phrase, mapping = m.group(1).strip(), m.group(2).strip()
                if len(phrase) > 2:
                    # Find the column from context
                    col_match = re.search(r"filter on (\w+) = ", hints)
                    col_name = col_match.group(1) if col_match else ""
                    if col_name:
                        glossary_terms.append((phrase, f"{col_name} = '{mapping}'"))

    if glossary_terms:
        parts.append("  <glossary>")
        for phrase, mapping in glossary_terms[:20]:
            parts.append(f'    <term phrase="{_esc(phrase)}" maps_to="{_esc(mapping)}"/>')
        parts.append("  </glossary>")

    parts.append("</data_context>")
    return "\n".join(parts)


def _esc(s: Any) -> str:
    """Escape XML special characters."""
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def _get_ai_reasoning(col: Any) -> str | None:
    """Extract reasoning text from a DatasetColumn's ai_suggestion."""
    if col.ai_suggestion and isinstance(col.ai_suggestion, dict):
        return col.ai_suggestion.get("reasoning")
    return None
