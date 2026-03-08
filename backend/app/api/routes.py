"""All API endpoints for DataBobIQ.

Route order matters for path-parameter disambiguation:
  /datasets/baseline  must be registered BEFORE /datasets/{dataset_id}
  /scenarios (list)   must be registered BEFORE /scenarios/{scenario_id}
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from pathlib import Path
from typing import Any, AsyncGenerator

import polars as pl
from fastapi import APIRouter, BackgroundTasks, Depends, Form, HTTPException, Query, UploadFile, File
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import settings
from app.database import get_db, sync_engine
from app.models.metadata import Dataset, DatasetColumn, DatasetRelationship, KnowledgeEntry, Scenario, SemanticColumn, SemanticValueLabel, TransformationStep
from app.schemas.api import (
    BaselineRequest,
    BaselineResponse,
    ChatRequest,
    DatasetColumnResponse,
    DatasetColumnUpdate,
    DatasetRelationshipCreate,
    DatasetRelationshipResponse,
    DatasetRelationshipUpdate,
    DatasetResponse,
    KnowledgeEntryCreate,
    KnowledgeEntryResponse,
    KnowledgeEntryUpdate,
    OrderByClause,
    QueryRequest,
    QueryResponse,
    RelationshipRef,
    ScenarioComputeRequest,
    ScenarioCreate,
    ScenarioResponse,
    ScenarioUpdate,
    SchemaResponse,
    SemanticColumnResponse,
    SemanticColumnUpdate,
    SemanticLabelBulkCreate,
    SemanticLayerResponse,
    TransformationStepCreate,
    TransformationStepResponse,
    TransformationPreviewResponse,
    TransformationSuggestRequest,
)
from app.services import calendar_svc
from app.services import parser as parser_svc
from app.services import scenario as scenario_svc
from app.services import schema_agent
from app.services import storage as storage_svc

logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _schema_response(dataset: Dataset) -> SchemaResponse:
    """Build a SchemaResponse from a fully-loaded Dataset ORM object."""
    rels = list(getattr(dataset, "source_relationships", []))
    rels += list(getattr(dataset, "target_relationships", []))
    dr = DatasetResponse.model_validate(dataset)
    return SchemaResponse(
        dataset=dr,
        columns=dr.columns,
        relationships=[DatasetRelationshipResponse.model_validate(r) for r in rels],
    )


def _compute_coverage(
    src_table: str, src_col: str, tgt_table: str, tgt_col: str
) -> tuple[int, int]:
    """Return (coverage_pct, overlap_count) between two dynamic table columns.

    coverage_pct = percentage of source values that exist in target.
    """
    try:
        src_df = storage_svc.read_dataset(sync_engine, src_table, columns=[src_col])
        tgt_df = storage_svc.read_dataset(sync_engine, tgt_table, columns=[tgt_col])
        src_vals = set(src_df[src_col].drop_nulls().to_list())
        tgt_vals = set(tgt_df[tgt_col].drop_nulls().to_list())
        if not src_vals:
            return 0, 0
        overlap = src_vals & tgt_vals
        pct = int(len(overlap) / len(src_vals) * 100)
        return pct, len(overlap)
    except Exception as exc:
        logger.warning("Coverage computation failed: %s", exc)
        return 0, 0


def _execute_query(
    table_name: str,
    columns: list[str] | None,
    filters: dict[str, Any] | None,
    group_by: list[str] | None,
    aggregations: dict[str, str] | None,
    order_by: list[OrderByClause] | None,
    limit: int,
) -> pl.DataFrame:
    """Fetch rows from a dynamic table, applying filters/group/agg in Polars."""
    # Read from storage (pushes basic filtering to SQL for performance)
    df = storage_svc.read_dataset(
        sync_engine,
        table_name,
        columns=columns,
        limit=None,  # we apply limit after Polars transforms
    )
    if df.is_empty():
        return df

    # Apply equality / IN filters in Polars
    if filters:
        for col_name, val in filters.items():
            if col_name not in df.columns:
                continue
            if isinstance(val, list):
                str_vals = [str(v) for v in val]
                df = df.filter(pl.col(col_name).cast(pl.Utf8).is_in(str_vals))
            else:
                df = df.filter(pl.col(col_name) == val)

    # Group-by + aggregations
    if group_by and aggregations:
        valid_gb = [c for c in group_by if c in df.columns]
        agg_exprs: list[pl.Expr] = []
        agg_map = {"sum": "sum", "avg": "mean", "mean": "mean",
                   "min": "min", "max": "max", "count": "count"}
        for col_name, func_name in aggregations.items():
            if col_name not in df.columns:
                continue
            fn = agg_map.get(func_name, "sum")
            agg_exprs.append(getattr(pl.col(col_name), fn)().alias(col_name))
        if valid_gb and agg_exprs:
            df = df.group_by(valid_gb).agg(agg_exprs)

    # Order
    if order_by:
        sort_cols = [o.column for o in order_by if o.column in df.columns]
        sort_desc = [o.direction == "desc" for o in order_by if o.column in df.columns]
        if sort_cols:
            df = df.sort(sort_cols, descending=sort_desc)

    # Limit
    if limit:
        df = df.head(limit)

    return df


def _df_to_query_response(df: pl.DataFrame) -> QueryResponse:
    """Serialize a Polars DataFrame to QueryResponse."""
    col_names = df.columns
    rows = []
    for row in df.iter_rows():
        rows.append([_json_safe(v) for v in row])
    return QueryResponse(columns=col_names, data=rows, total_rows=len(rows))


def _json_safe(v: Any) -> Any:
    """Coerce non-JSON-serialisable scalars."""
    import math
    from datetime import date, datetime
    from decimal import Decimal
    if isinstance(v, float) and math.isnan(v):
        return None
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    if isinstance(v, Decimal):
        # PostgreSQL Numeric columns come back as Decimal; send as float so JS
        # receives a number, not a string (which breaks variance/chart math).
        f = float(v)
        return None if math.isnan(f) else f
    return v


async def _build_baseline_df(
    fact_dataset: Dataset,
    rel_refs: list[RelationshipRef],
    db: AsyncSession,
) -> pl.DataFrame:
    """Join fact table with dimension tables for all specified relationships."""
    df = storage_svc.read_dataset(sync_engine, fact_dataset.table_name)

    for ref in rel_refs:
        result = await db.execute(
            select(DatasetRelationship).where(DatasetRelationship.id == ref.rel_id)
        )
        rel = result.scalar_one_or_none()
        if rel is None:
            logger.warning("Relationship %s not found, skipping", ref.rel_id)
            continue

        # Determine which side is the fact (source) and which the dimension (target)
        if rel.source_dataset_id == fact_dataset.id:
            fact_col = rel.source_column
            dim_dataset_id = rel.target_dataset_id
            dim_col = rel.target_column
        elif rel.target_dataset_id == fact_dataset.id:
            fact_col = rel.target_column
            dim_dataset_id = rel.source_dataset_id
            dim_col = rel.source_column
        else:
            logger.warning("Relationship %s does not reference fact dataset, skipping", ref.rel_id)
            continue

        dim_result = await db.execute(select(Dataset).where(Dataset.id == dim_dataset_id))
        dim_dataset = dim_result.scalar_one_or_none()
        if dim_dataset is None:
            continue

        dim_df = storage_svc.read_dataset(sync_engine, dim_dataset.table_name)

        # Avoid _row_id collision from dimension table
        dim_df = dim_df.drop("_row_id", strict=False)

        # Rename clashing non-join columns with table prefix
        dim_name = dim_dataset.name
        rename_map: dict[str, str] = {}
        for c in dim_df.columns:
            if c != dim_col and c in df.columns:
                rename_map[c] = f"{dim_name}__{c}"
        if rename_map:
            dim_df = dim_df.rename(rename_map)

        try:
            # Cast join keys to Utf8 so type mismatches (e.g. pl.Date vs pl.Utf8) don't break the join
            if df[fact_col].dtype != pl.Utf8:
                df = df.with_columns(pl.col(fact_col).cast(pl.Utf8, strict=False))
            if dim_df[dim_col].dtype != pl.Utf8:
                dim_df = dim_df.with_columns(pl.col(dim_col).cast(pl.Utf8, strict=False))
            df = df.join(dim_df, left_on=fact_col, right_on=dim_col, how="left")
        except Exception as exc:
            logger.warning("Join on %s.%s failed: %s", dim_dataset.name, dim_col, exc)

    return df




async def _auto_populate_labels_from_relationship(
    db: AsyncSession, rel: DatasetRelationship
) -> None:
    """Auto-populate SemanticValueLabel rows from a dimension table.

    When fact.key_col → dim.key_col and the dimension also has a text
    attribute column (e.g. 'bezeichnung', 'name'), we create value labels
    so `400100` maps to 'Personnel Costs' in the semantic layer.
    """
    src_ds_r = await db.execute(
        select(Dataset).where(Dataset.id == rel.source_dataset_id).options(selectinload(Dataset.columns))
    )
    tgt_ds_r = await db.execute(
        select(Dataset).where(Dataset.id == rel.target_dataset_id).options(selectinload(Dataset.columns))
    )
    src = src_ds_r.scalar_one_or_none()
    tgt = tgt_ds_r.scalar_one_or_none()
    if not src or not tgt:
        return

    src_has_measures = any(c.column_role == "measure" for c in src.columns)
    tgt_has_measures = any(c.column_role == "measure" for c in tgt.columns)

    if src_has_measures and not tgt_has_measures:
        fact_ds, fact_col = src, rel.source_column
        dim_ds, dim_key = tgt, rel.target_column
    elif tgt_has_measures and not src_has_measures:
        fact_ds, fact_col = tgt, rel.target_column
        dim_ds, dim_key = src, rel.source_column
    else:
        return  # can't determine fact/dimension

    # Find the best descriptive text column in the dimension table
    desc_candidates = [
        c for c in dim_ds.columns
        if c.column_role == "attribute" and c.data_type == "text"
        and c.column_name != dim_key
    ]
    if not desc_candidates:
        return

    name_patterns = ["name", "bezeichnung", "description", "label", "text", "desc", "title"]
    def _score(col: DatasetColumn) -> int:
        for i, pat in enumerate(name_patterns):
            if pat in col.column_name.lower():
                return i
        return 100

    desc_candidates.sort(key=_score)
    desc_col = desc_candidates[0]

    try:
        dim_df = storage_svc.read_dataset(
            sync_engine, dim_ds.table_name, columns=[dim_key, desc_col.column_name]
        )
    except Exception as exc:
        logger.warning("_auto_populate_labels: failed to read dim table %s: %s", dim_ds.table_name, exc)
        return

    if dim_df.is_empty():
        return

    # Create or get SemanticColumn for the fact column
    existing_sc = await db.execute(
        select(SemanticColumn).where(
            SemanticColumn.dataset_id == fact_ds.id,
            SemanticColumn.column_name == fact_col,
        )
    )
    sem_col = existing_sc.scalar_one_or_none()
    if not sem_col:
        sem_col = SemanticColumn(
            id=uuid.uuid4().hex,
            dataset_id=fact_ds.id,
            column_name=fact_col,
            description=f"Links to {dim_ds.name}.{desc_col.column_name}",
            synonyms=[],
            value_source=f"{dim_ds.name}.{desc_col.column_name}",
        )
        db.add(sem_col)
        await db.flush()

    # Upsert value labels
    label_count = 0
    for row in dim_df.iter_rows(named=True):
        raw = row.get(dim_key)
        label = row.get(desc_col.column_name)
        if raw is None or label is None:
            continue
        raw_str = str(raw)
        existing_lbl = await db.execute(
            select(SemanticValueLabel).where(
                SemanticValueLabel.semantic_column_id == sem_col.id,
                SemanticValueLabel.raw_value == raw_str,
            )
        )
        if existing_lbl.scalar_one_or_none():
            continue
        db.add(SemanticValueLabel(
            id=uuid.uuid4().hex,
            semantic_column_id=sem_col.id,
            raw_value=raw_str,
            display_label=str(label),
        ))
        label_count += 1

    await db.commit()
    logger.info(
        "Auto-populated %d value labels for %s.%s from %s.%s",
        label_count, fact_ds.name, fact_col, dim_ds.name, desc_col.column_name,
    )

    # Second pass: create SemanticColumn entries for all text attribute columns
    # in the dimension table (grouping columns like reporting_h2 that appear
    # in the fact baseline after the join).
    for dim_col_meta in dim_ds.columns:
        if dim_col_meta.column_role not in ("attribute",) or dim_col_meta.data_type != "text":
            continue
        if dim_col_meta.column_name == dim_key:
            continue  # skip the join key
        # Check if this column already has a semantic entry on the fact dataset
        existing_sem_r = await db.execute(
            select(SemanticColumn).where(
                SemanticColumn.dataset_id == fact_ds.id,
                SemanticColumn.column_name == dim_col_meta.column_name,
            )
        )
        if existing_sem_r.scalar_one_or_none():
            continue
        try:
            dim_col_df = storage_svc.read_dataset(
                sync_engine, dim_ds.table_name, columns=[dim_col_meta.column_name]
            )
            unique_vals = (
                dim_col_df[dim_col_meta.column_name]
                .drop_nulls().unique().sort().to_list()
            )
        except Exception:
            unique_vals = []
        val_preview = ", ".join(str(v) for v in unique_vals[:10])
        if len(unique_vals) > 10:
            val_preview += "..."
        db.add(SemanticColumn(
            id=uuid.uuid4().hex,
            dataset_id=fact_ds.id,
            column_name=dim_col_meta.column_name,
            description=(
                f"Grouping column from {dim_ds.name} dimension (joined via {dim_key}). "
                f"Values: {val_preview}"
            ),
            synonyms=[],
            value_source=f"{dim_ds.name}.{dim_col_meta.column_name}",
        ))
    await db.commit()
    logger.info(
        "Auto-created SemanticColumn entries for attribute columns of dim %s", dim_ds.name
    )


async def _run_agent_and_persist(dataset_id: str) -> None:
    """Background task: run schema agent and persist results."""
    from app.database import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Dataset)
            .where(Dataset.id == dataset_id)
            .options(selectinload(Dataset.columns))
        )
        dataset = result.scalar_one_or_none()
        if dataset is None:
            logger.warning("_run_agent_and_persist: dataset %s not found", dataset_id)
            return

        # Load all active datasets so the agent can suggest cross-table relationships
        all_ds_result = await db.execute(
            select(Dataset)
            .where(Dataset.status != "deleted")
            .options(selectinload(Dataset.columns))
        )
        all_datasets = all_ds_result.scalars().all()

        tables_payload = [
            {
                "name": ds.name,
                "filename": ds.source_filename or "",
                "row_count": ds.row_count,
                "columns": [
                    {
                        "column_name": c.column_name,
                        "data_type": c.data_type,
                        "column_role": c.column_role,
                        "unique_count": c.unique_count,
                        "null_count": 0,
                        "sample_values": c.sample_values or [],
                    }
                    for c in ds.columns
                ],
                "preview_rows": [],
            }
            for ds in all_datasets
        ]

        try:
            agent_result = await asyncio.wait_for(
                schema_agent.analyze_schema(tables_payload), timeout=300
            )
        except asyncio.TimeoutError:
            logger.warning("Schema agent timed out for dataset %s", dataset_id)
            dataset.ai_analyzed = True
            dataset.ai_notes = {"description": "", "table_type": "unknown",
                                 "warnings": ["AI analysis timed out"]}
            await db.commit()
            return
        except Exception as exc:
            logger.warning("Schema agent failed for dataset %s: %s", dataset_id, exc)
            dataset.ai_analyzed = True
            dataset.ai_notes = {"description": "", "table_type": "unknown",
                                 "warnings": [f"AI analysis failed: {exc}"]}
            await db.commit()
            return

        agent_tables = {t["name"]: t for t in agent_result.get("tables", [])}
        agent_table = agent_tables.get(dataset.name, {})

        _, ai_notes = schema_agent.merge_agent_results([], agent_table)
        ai_notes["relationships"] = agent_result.get("suggested_relationships", [])
        ai_notes["warnings"] = agent_result.get("warnings", [])

        dataset.ai_notes = ai_notes
        dataset.ai_analyzed = True

        agent_col_map = {c["column_name"]: c for c in agent_table.get("columns", [])}
        for col in dataset.columns:
            agent_col = agent_col_map.get(col.column_name)
            if agent_col:
                if col.column_role == "attribute":
                    col.column_role = agent_col.get("suggested_role", col.column_role)
                col.display_name = agent_col.get("suggested_display_name", col.display_name)
                col.ai_suggestion = {
                    "suggested_role": agent_col.get("suggested_role"),
                    "suggested_display_name": agent_col.get("suggested_display_name"),
                    "reasoning": agent_col.get("reasoning"),
                }

        # Persist agent_context_notes
        if agent_table.get("agent_context_notes"):
            dataset.agent_context_notes = agent_table["agent_context_notes"]

        # Persist value_label_suggestions from agent
        for label_suggestion in agent_table.get("value_label_suggestions", []):
            col_name = label_suggestion.get("column_name")
            labels = label_suggestion.get("labels", [])
            if not col_name or not labels:
                continue
            existing_sc = await db.execute(
                select(SemanticColumn).where(
                    SemanticColumn.dataset_id == dataset.id,
                    SemanticColumn.column_name == col_name,
                )
            )
            sem_col = existing_sc.scalar_one_or_none()
            if not sem_col:
                sem_col = SemanticColumn(
                    id=uuid.uuid4().hex,
                    dataset_id=dataset.id,
                    column_name=col_name,
                    synonyms=[],
                )
                db.add(sem_col)
                await db.flush()
            for lbl in labels:
                raw_str = str(lbl.get("raw_value", ""))
                if not raw_str:
                    continue
                existing_lbl = await db.execute(
                    select(SemanticValueLabel).where(
                        SemanticValueLabel.semantic_column_id == sem_col.id,
                        SemanticValueLabel.raw_value == raw_str,
                    )
                )
                if existing_lbl.scalar_one_or_none():
                    continue
                db.add(SemanticValueLabel(
                    id=uuid.uuid4().hex,
                    semantic_column_id=sem_col.id,
                    raw_value=raw_str,
                    display_label=str(lbl.get("display_label", raw_str)),
                    category=lbl.get("category"),
                ))

        # Also persist column descriptions from agent reasoning
        for agent_col in agent_table.get("columns", []):
            col_name = agent_col.get("column_name")
            if not col_name or not agent_col.get("reasoning"):
                continue
            existing_sc = await db.execute(
                select(SemanticColumn).where(
                    SemanticColumn.dataset_id == dataset.id,
                    SemanticColumn.column_name == col_name,
                )
            )
            sem_col = existing_sc.scalar_one_or_none()
            if sem_col:
                if not sem_col.description:
                    sem_col.description = agent_col["reasoning"]
            else:
                db.add(SemanticColumn(
                    id=uuid.uuid4().hex,
                    dataset_id=dataset.id,
                    column_name=col_name,
                    description=agent_col["reasoning"],
                    synonyms=[],
                ))

        # Persist AI-suggested transformations (only when no existing grouping column covers the need)
        suggested_transforms = agent_table.get("suggested_transformations", [])
        if suggested_transforms:
            # Get current step_order max for this dataset
            existing_steps = await db.execute(
                select(TransformationStep).where(TransformationStep.dataset_id == dataset.id)
            )
            current_steps = existing_steps.scalars().all()
            next_order = (max((s.step_order for s in current_steps), default=-1) + 1) if current_steps else 0

            for i, sug in enumerate(suggested_transforms):
                if not sug.get("definition") or not sug.get("name"):
                    continue
                defn = sug["definition"]
                # Ensure step_type is in the definition
                if "step_type" not in defn:
                    defn["step_type"] = sug.get("step_type", "reclassification")
                db.add(TransformationStep(
                    id=uuid.uuid4().hex,
                    dataset_id=dataset.id,
                    step_order=next_order + i,
                    step_type=sug.get("step_type", "reclassification"),
                    name=sug["name"],
                    description=sug.get("description"),
                    definition=defn,
                    status="pending",
                    created_by="ai_agent",
                    ai_prompt=sug.get("reason"),
                ))
            logger.info("Created %d pending transformation suggestions for dataset %s", len(suggested_transforms), dataset_id)

        await db.commit()
        logger.info("Schema agent analysis persisted for dataset %s", dataset_id)


# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------

@router.post("/upload", response_model=list[DatasetResponse], status_code=201)
async def upload_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    name: str | None = Form(default=None),
    db: AsyncSession = Depends(get_db),
):
    """Upload a CSV or Excel file.

    Parses all sheets, creates a dynamic PostgreSQL table per sheet, loads
    data, detects cross-sheet relationships, and fires the AI schema agent
    in the background.  Returns one ``DatasetResponse`` per sheet.
    """
    upload_dir = Path(settings.UPLOAD_DIR)
    upload_dir.mkdir(parents=True, exist_ok=True)

    safe_name = f"{uuid.uuid4().hex}_{file.filename}"
    file_path = upload_dir / Path(safe_name)
    content = await file.read()
    file_path.write_bytes(content)

    # --- Parse all sheets ---
    try:
        parse_result = parser_svc.parse_file(str(file_path))
    except Exception as exc:
        file_path.unlink(missing_ok=True)
        raise HTTPException(status_code=422, detail=f"Failed to parse file: {exc}") from exc

    sheets = parse_result.get("sheets", [])
    if not sheets:
        file_path.unlink(missing_ok=True)
        raise HTTPException(status_code=422, detail="No sheets/tables found in the file")

    created_datasets: list[Dataset] = []
    sheet_dfs: dict[str, pl.DataFrame] = {}

    for sheet in sheets:
        sheet_name: str = sheet["name"]
        df: pl.DataFrame = sheet["data"]
        parsed_columns: list = sheet["columns"]

        table_name = storage_svc.generate_table_name()
        columns_meta = [
            {
                "column_name": col.column_name,
                "data_type": col.data_type,
                "column_role": col.column_role,
                "unique_count": col.unique_count,
                "display_name": col.display_name or col.column_name,
            }
            for col in parsed_columns
        ]

        dataset_display_name = (
            name
            if (name and len(sheets) == 1)
            else f"{name or Path(file.filename).stem} – {sheet_name}"
            if len(sheets) > 1
            else name or Path(file.filename).stem
        )

        # --- DDL + load ---
        try:
            storage_svc.create_dataset_table(sync_engine, table_name, columns_meta)
            row_count = storage_svc.load_data(sync_engine, table_name, df, columns_meta)
        except Exception as exc:
            storage_svc.drop_dataset_table(sync_engine, table_name)
            logger.error("Failed to load sheet %r: %s", sheet_name, exc)
            # Mark existing successful datasets as errored and raise
            for ds in created_datasets:
                ds.status = "error"
            await db.commit()
            raise HTTPException(status_code=500, detail=f"Failed to load data: {exc}") from exc

        dataset = Dataset(
            id=uuid.uuid4().hex,
            name=dataset_display_name,
            table_name=table_name,
            source_filename=file.filename,
            row_count=row_count,
            status="active",
            ai_analyzed=False,
        )
        db.add(dataset)

        for col in parsed_columns:
            db.add(
                DatasetColumn(
                    id=uuid.uuid4().hex,
                    dataset_id=dataset.id,
                    column_name=col.column_name,
                    display_name=col.display_name or col.column_name,
                    data_type=col.data_type,
                    column_role=col.column_role,
                    unique_count=col.unique_count,
                    sample_values=col.sample_values,
                )
            )

        created_datasets.append(dataset)
        sheet_dfs[sheet_name] = df

    await db.commit()

    # --- Auto-detect cross-sheet relationships ---
    if len(sheet_dfs) > 1:
        detected_rels = parser_svc.detect_relationships(sheet_dfs)
        ds_by_name = {ds.name.split(" – ")[-1]: ds for ds in created_datasets}
        new_rels: list[DatasetRelationship] = []
        for rel in detected_rels:
            src_ds = ds_by_name.get(rel["source"])
            tgt_ds = ds_by_name.get(rel["target"])
            if src_ds and tgt_ds:
                new_rel = DatasetRelationship(
                    id=uuid.uuid4().hex,
                    source_dataset_id=src_ds.id,
                    target_dataset_id=tgt_ds.id,
                    source_column=rel["source_col"],
                    target_column=rel["target_col"],
                    coverage_pct=rel.get("coverage"),
                    overlap_count=rel.get("overlap"),
                )
                db.add(new_rel)
                new_rels.append(new_rel)
        await db.commit()
        # Auto-populate semantic labels for detected relationships
        for new_rel in new_rels:
            try:
                await _auto_populate_labels_from_relationship(db, new_rel)
            except Exception as exc:
                logger.warning("Auto-populate labels failed for upload rel %s: %s", new_rel.id, exc)

    # Reload with columns relationship populated; auto-link to calendar
    responses: list[DatasetResponse] = []
    for ds in created_datasets:
        result = await db.execute(
            select(Dataset)
            .where(Dataset.id == ds.id)
            .options(selectinload(Dataset.columns))
        )
        loaded = result.scalar_one()
        responses.append(DatasetResponse.model_validate(loaded))

        await calendar_svc.auto_link_calendar(db, loaded)

        if settings.ANTHROPIC_API_KEY_AGENT:
            background_tasks.add_task(_run_agent_and_persist, ds.id)

    return responses


# ---------------------------------------------------------------------------
# Datasets — list + baseline (BEFORE /{dataset_id} to avoid param capture)
# ---------------------------------------------------------------------------

@router.get("/datasets", response_model=list[SchemaResponse])
async def list_datasets(db: AsyncSession = Depends(get_db)):
    """Return all non-deleted datasets with columns and relationships."""
    result = await db.execute(
        select(Dataset)
        .where(Dataset.status != "deleted")
        .options(
            selectinload(Dataset.columns),
            selectinload(Dataset.source_relationships),
            selectinload(Dataset.target_relationships),
        )
        .order_by(Dataset.created_at.desc())
    )
    return [_schema_response(d) for d in result.scalars().all()]


@router.get("/datasets/{dataset_id}/available-periods")
async def get_available_periods(dataset_id: str, db: AsyncSession = Depends(get_db)):
    """Return the sorted list of distinct period values for a dataset.

    Useful for populating projection source/target year selectors in the UI.
    Returns: {"periods": ["2024-01", ...], "years": ["2024", ...], "period_column": "period"}
    """
    result = await db.execute(
        select(Dataset)
        .where(Dataset.id == dataset_id, Dataset.status != "deleted")
        .options(selectinload(Dataset.columns))
    )
    dataset = result.scalar_one_or_none()
    if dataset is None:
        raise HTTPException(status_code=404, detail="Dataset not found")

    # Find a time-role column
    time_cols = [c.column_name for c in dataset.columns if c.column_role == "time"]
    period_candidates = list(scenario_svc._PERIOD_COLUMN_CANDIDATES)
    period_col: str | None = None

    # Prefer explicitly time-role column, fall back to name heuristic
    for tc in time_cols:
        period_col = tc
        break
    if not period_col:
        col_names_lower = {c.column_name.lower(): c.column_name for c in dataset.columns}
        for candidate in period_candidates:
            if candidate in col_names_lower:
                period_col = col_names_lower[candidate]
                break

    if not period_col:
        return {"periods": [], "years": [], "period_column": None}

    try:
        df = storage_svc.read_dataset(sync_engine, dataset.table_name, columns=[period_col])
        periods = sorted(df[period_col].cast(pl.Utf8).drop_nulls().unique().to_list())
        years = sorted(set(p[:4] for p in periods if len(p) >= 4))
        return {"periods": periods, "years": years, "period_column": period_col}
    except Exception as exc:
        logger.warning("Could not read periods for dataset %s: %s", dataset_id, exc)
        return {"periods": [], "years": [], "period_column": None}


@router.post("/datasets/baseline", response_model=BaselineResponse)
async def build_baseline(body: BaselineRequest, db: AsyncSession = Depends(get_db)):
    """Build the enriched baseline by joining a fact table with dimension tables.

    Returns the flat, joined result as JSON rows ready for the frontend chart
    engine.  Complex transforms happen in Polars — SQL stays simple.
    """
    result = await db.execute(
        select(Dataset).where(
            Dataset.id == body.fact_dataset_id,
            Dataset.status != "deleted",
        )
    )
    fact_ds = result.scalar_one_or_none()
    if fact_ds is None:
        raise HTTPException(status_code=404, detail="Fact dataset not found")

    df = await _build_baseline_df(fact_ds, body.relationships, db)

    col_names = df.columns
    rows = [[_json_safe(v) for v in row] for row in df.iter_rows()]
    return BaselineResponse(columns=col_names, data=rows, row_count=len(rows))


# ---------------------------------------------------------------------------
# Datasets — single-resource endpoints
# ---------------------------------------------------------------------------

@router.get("/datasets/{dataset_id}", response_model=SchemaResponse)
async def get_dataset(dataset_id: str, db: AsyncSession = Depends(get_db)):
    """Return a single dataset with full schema (columns + relationships)."""
    result = await db.execute(
        select(Dataset)
        .where(Dataset.id == dataset_id, Dataset.status != "deleted")
        .options(
            selectinload(Dataset.columns),
            selectinload(Dataset.source_relationships),
            selectinload(Dataset.target_relationships),
        )
    )
    dataset = result.scalar_one_or_none()
    if dataset is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return _schema_response(dataset)


@router.delete("/datasets/{dataset_id}", status_code=204)
async def delete_dataset(
    dataset_id: str,
    hard: bool = Query(default=False),
    db: AsyncSession = Depends(get_db),
):
    """Delete a dataset.

    ``?hard=true`` physically drops the dynamic table; default is soft-delete
    (status set to "deleted") so data can be recovered.
    """
    result = await db.execute(
        select(Dataset).where(Dataset.id == dataset_id, Dataset.status != "deleted")
    )
    dataset = result.scalar_one_or_none()
    if dataset is None:
        raise HTTPException(status_code=404, detail="Dataset not found")

    if isinstance(dataset.ai_notes, dict) and dataset.ai_notes.get("is_system"):
        raise HTTPException(status_code=403, detail="System datasets cannot be deleted.")

    if hard:
        storage_svc.drop_dataset_table(sync_engine, dataset.table_name)
        await db.delete(dataset)
    else:
        dataset.status = "deleted"

    await db.commit()


@router.patch("/datasets/{dataset_id}/columns/{column_id}", response_model=DatasetColumnResponse)
async def update_column(
    dataset_id: str,
    column_id: str,
    body: DatasetColumnUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Update a column's role or display name.

    This is the primary way users correct auto-detected schema in the UI.
    """
    result = await db.execute(
        select(DatasetColumn).where(
            DatasetColumn.id == column_id,
            DatasetColumn.dataset_id == dataset_id,
        )
    )
    col = result.scalar_one_or_none()
    if col is None:
        raise HTTPException(status_code=404, detail="Column not found")

    if body.column_role is not None:
        col.column_role = body.column_role
    if body.display_name is not None:
        col.display_name = body.display_name

    await db.commit()
    await db.refresh(col)
    return DatasetColumnResponse.model_validate(col)


@router.post("/datasets/{dataset_id}/query", response_model=QueryResponse)
async def query_dataset(
    dataset_id: str,
    body: QueryRequest,
    db: AsyncSession = Depends(get_db),
):
    """Execute a flexible query against a dataset's dynamic table.

    When ``group_by`` + ``aggregations`` are provided the result is aggregated
    in Polars.  Without them, raw rows are returned (max 10 000).
    """
    result = await db.execute(
        select(Dataset).where(Dataset.id == dataset_id, Dataset.status != "deleted")
    )
    dataset = result.scalar_one_or_none()
    if dataset is None:
        raise HTTPException(status_code=404, detail="Dataset not found")

    try:
        df = _execute_query(
            table_name=dataset.table_name,
            columns=body.columns,
            filters=body.filters,
            group_by=body.group_by,
            aggregations=body.aggregations,
            order_by=body.order_by,
            limit=body.limit,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Query failed: {exc}") from exc

    return _df_to_query_response(df)


@router.post("/datasets/{dataset_id}/reanalyze", response_model=DatasetResponse)
async def reanalyze_dataset(
    dataset_id: str,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    """Retry AI schema analysis for a dataset (non-blocking, runs in background)."""
    result = await db.execute(
        select(Dataset)
        .where(Dataset.id == dataset_id)
        .options(selectinload(Dataset.columns))
    )
    dataset = result.scalar_one_or_none()
    if dataset is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    if not settings.ANTHROPIC_API_KEY_AGENT:
        raise HTTPException(status_code=503, detail="ANTHROPIC_API_KEY_AGENT is not configured")

    background_tasks.add_task(_run_agent_and_persist, dataset_id)
    return DatasetResponse.model_validate(dataset)


# ---------------------------------------------------------------------------
# Relationships
# ---------------------------------------------------------------------------

@router.post("/relationships", response_model=DatasetRelationshipResponse, status_code=201)
async def create_relationship(
    body: DatasetRelationshipCreate,
    db: AsyncSession = Depends(get_db),
):
    """Create a join relationship between two datasets.

    Coverage and overlap statistics are computed automatically by inspecting
    the actual values in both dynamic tables.
    """
    # Verify both datasets exist
    for ds_id in (body.source_dataset_id, body.target_dataset_id):
        r = await db.execute(select(Dataset).where(Dataset.id == ds_id))
        if r.scalar_one_or_none() is None:
            raise HTTPException(status_code=404, detail=f"Dataset {ds_id} not found")

    src_r = await db.execute(select(Dataset).where(Dataset.id == body.source_dataset_id))
    tgt_r = await db.execute(select(Dataset).where(Dataset.id == body.target_dataset_id))
    src_ds = src_r.scalar_one()
    tgt_ds = tgt_r.scalar_one()

    coverage_pct, overlap_count = _compute_coverage(
        src_ds.table_name, body.source_column,
        tgt_ds.table_name, body.target_column,
    )

    rel = DatasetRelationship(
        id=uuid.uuid4().hex,
        source_dataset_id=body.source_dataset_id,
        target_dataset_id=body.target_dataset_id,
        source_column=body.source_column,
        target_column=body.target_column,
        coverage_pct=coverage_pct,
        overlap_count=overlap_count,
    )
    db.add(rel)
    await db.commit()
    await db.refresh(rel)

    # Auto-populate semantic value labels from dimension table
    try:
        await _auto_populate_labels_from_relationship(db, rel)
    except Exception as exc:
        logger.warning("Auto-populate labels failed for rel %s: %s", rel.id, exc)

    return DatasetRelationshipResponse.model_validate(rel)


@router.put("/relationships/{rel_id}", response_model=DatasetRelationshipResponse)
async def update_relationship(
    rel_id: str,
    body: DatasetRelationshipUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Update source/target column for a relationship and recompute coverage."""
    result = await db.execute(
        select(DatasetRelationship).where(DatasetRelationship.id == rel_id)
    )
    rel = result.scalar_one_or_none()
    if rel is None:
        raise HTTPException(status_code=404, detail="Relationship not found")

    if body.source_column is not None:
        rel.source_column = body.source_column
    if body.target_column is not None:
        rel.target_column = body.target_column

    src_r = await db.execute(select(Dataset).where(Dataset.id == rel.source_dataset_id))
    tgt_r = await db.execute(select(Dataset).where(Dataset.id == rel.target_dataset_id))
    src_ds = src_r.scalar_one()
    tgt_ds = tgt_r.scalar_one()

    coverage_pct, overlap_count = _compute_coverage(
        src_ds.table_name, rel.source_column,
        tgt_ds.table_name, rel.target_column,
    )
    rel.coverage_pct = coverage_pct
    rel.overlap_count = overlap_count

    await db.commit()
    await db.refresh(rel)
    return DatasetRelationshipResponse.model_validate(rel)


@router.delete("/relationships/{rel_id}", status_code=204)
async def delete_relationship(rel_id: str, db: AsyncSession = Depends(get_db)):
    """Delete a relationship."""
    result = await db.execute(
        select(DatasetRelationship).where(DatasetRelationship.id == rel_id)
    )
    rel = result.scalar_one_or_none()
    if rel is None:
        raise HTTPException(status_code=404, detail="Relationship not found")
    await db.delete(rel)
    await db.commit()


# ---------------------------------------------------------------------------
# Scenarios — list + create (BEFORE /{scenario_id})
# ---------------------------------------------------------------------------

@router.get("/scenarios", response_model=list[ScenarioResponse])
async def list_scenarios(
    dataset_id: str | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    """List scenarios, optionally filtered by dataset_id."""
    q = select(Scenario).order_by(Scenario.created_at.desc())
    if dataset_id:
        q = q.where(Scenario.dataset_id == dataset_id)
    result = await db.execute(q)
    return [ScenarioResponse.model_validate(s) for s in result.scalars().all()]


@router.post("/scenarios", response_model=ScenarioResponse, status_code=201)
async def create_scenario(body: ScenarioCreate, db: AsyncSession = Depends(get_db)):
    """Create a new what-if scenario."""
    r = await db.execute(select(Dataset).where(Dataset.id == body.dataset_id))
    if r.scalar_one_or_none() is None:
        raise HTTPException(status_code=404, detail="Dataset not found")

    scenario = Scenario(
        id=uuid.uuid4().hex,
        name=body.name,
        dataset_id=body.dataset_id,
        rules=body.rules,
        color=body.color,
        base_config=(
            body.base_config.model_dump() if hasattr(body.base_config, 'model_dump')
            else body.base_config if isinstance(body.base_config, dict)
            else None
        ) if body.base_config else None,
    )
    db.add(scenario)
    await db.commit()
    await db.refresh(scenario)
    return ScenarioResponse.model_validate(scenario)


# ---------------------------------------------------------------------------
# Scenarios — single-resource endpoints
# ---------------------------------------------------------------------------

@router.get("/scenarios/{scenario_id}", response_model=ScenarioResponse)
async def get_scenario(scenario_id: str, db: AsyncSession = Depends(get_db)):
    """Get a single scenario by ID."""
    result = await db.execute(select(Scenario).where(Scenario.id == scenario_id))
    scenario = result.scalar_one_or_none()
    if scenario is None:
        raise HTTPException(status_code=404, detail="Scenario not found")
    return ScenarioResponse.model_validate(scenario)


@router.put("/scenarios/{scenario_id}", response_model=ScenarioResponse)
async def update_scenario(
    scenario_id: str,
    body: ScenarioUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Update scenario name, rules, or color."""
    result = await db.execute(select(Scenario).where(Scenario.id == scenario_id))
    scenario = result.scalar_one_or_none()
    if scenario is None:
        raise HTTPException(status_code=404, detail="Scenario not found")

    if body.name is not None:
        scenario.name = body.name
    if body.rules is not None:
        scenario.rules = body.rules
    if body.color is not None:
        scenario.color = body.color
    if body.base_config is not None:
        if hasattr(body.base_config, 'model_dump'):
            scenario.base_config = body.base_config.model_dump()
        elif isinstance(body.base_config, dict):
            scenario.base_config = body.base_config
        else:
            scenario.base_config = None

    await db.commit()
    await db.refresh(scenario)
    return ScenarioResponse.model_validate(scenario)


@router.delete("/scenarios/{scenario_id}", status_code=204)
async def delete_scenario(scenario_id: str, db: AsyncSession = Depends(get_db)):
    """Delete a scenario."""
    result = await db.execute(select(Scenario).where(Scenario.id == scenario_id))
    scenario = result.scalar_one_or_none()
    if scenario is None:
        raise HTTPException(status_code=404, detail="Scenario not found")
    await db.delete(scenario)
    await db.commit()


@router.post("/scenarios/{scenario_id}/compute")
async def compute_scenario(
    scenario_id: str,
    body: ScenarioComputeRequest,
    db: AsyncSession = Depends(get_db),
):
    """Apply scenario rules to a baseline and return full comparison data.

    1. Build enriched baseline (fact + dimension joins via Polars).
    2. Apply rules sequentially via the Polars scenario engine.
    3. Return:
       - ``baseline``  – raw rows before rules
       - ``scenario``  – raw rows after rules
       - ``variance``  – grouped delta/delta_pct (uses ``group_by`` from request)
       - ``waterfall`` – waterfall steps (uses ``breakdown_field`` from request)
    """
    result = await db.execute(select(Scenario).where(Scenario.id == scenario_id))
    scenario = result.scalar_one_or_none()
    if scenario is None:
        raise HTTPException(status_code=404, detail="Scenario not found")

    fact_result = await db.execute(
        select(Dataset).where(
            Dataset.id == body.fact_dataset_id,
            Dataset.status != "deleted",
        )
    )
    fact_ds = fact_result.scalar_one_or_none()
    if fact_ds is None:
        raise HTTPException(status_code=404, detail="Fact dataset not found")

    value_col: str = body.value_column or "amount"

    baseline_df = await _build_baseline_df(fact_ds, body.relationships, db)

    # Detect an amount column if the requested one is absent
    if value_col not in baseline_df.columns:
        numeric_cols = [
            c for c in baseline_df.columns
            if baseline_df[c].dtype in (pl.Float32, pl.Float64, pl.Int32, pl.Int64)
        ]
        if not numeric_cols:
            raise HTTPException(
                status_code=422,
                detail=f"value_column '{value_col}' not found and no numeric column available",
            )
        value_col = numeric_cols[0]
        logger.info("compute_scenario: using %r as value_column", value_col)

    # Load ALL scenarios for this dataset (needed for chained baselines)
    all_sc_result = await db.execute(
        select(Scenario).where(Scenario.dataset_id == scenario.dataset_id)
    )
    all_scenarios = {
        s.id: {"rules": s.rules, "base_config": s.base_config}
        for s in all_sc_result.scalars().all()
    }

    try:
        scenario_df = scenario_svc.compute_scenario_output(
            baseline_df, scenario.rules, scenario.base_config,
            all_scenarios, value_col,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Scenario computation failed: {exc}") from exc

    # Determine actual vs projected periods for the response
    period_col = scenario_svc._find_period_col(scenario_df)
    has_projection = "_data_source" in scenario_df.columns and "projected" in scenario_df["_data_source"].to_list()
    actuals_periods: list[str] = []
    projection_periods: list[str] = []
    if period_col and "_data_source" in scenario_df.columns:
        actuals_periods = sorted(
            scenario_df.filter(pl.col("_data_source") == "actual")[period_col]
            .cast(pl.Utf8).drop_nulls().unique().to_list()
        )
        projection_periods = sorted(
            scenario_df.filter(pl.col("_data_source") == "projected")[period_col]
            .cast(pl.Utf8).drop_nulls().unique().to_list()
        )

    # For baseline comparison, use only actual rows (matching original baseline_df)
    baseline_for_variance = baseline_df
    if "_data_source" in scenario_df.columns:
        # actual baseline rows have _data_source="actual" in scenario_df
        pass  # baseline_df already lacks _data_source column

    def _rows(df: pl.DataFrame) -> list[list]:
        return [[_json_safe(v) for v in row] for row in df.iter_rows()]

    response: dict = {
        "scenario_id": scenario_id,
        "value_column": value_col,
        "columns": scenario_df.columns,
        "baseline": _rows(baseline_df),
        "scenario": _rows(scenario_df),
        "row_count": len(scenario_df),
        "has_projection": has_projection,
        "actuals_periods": actuals_periods,
        "projection_periods": projection_periods,
    }

    if body.group_by:
        response["variance"] = scenario_svc.compute_variance(
            baseline_df, scenario_df, body.group_by, value_col
        )

    if body.breakdown_field:
        response["waterfall"] = scenario_svc.compute_waterfall(
            baseline_df, scenario_df, body.breakdown_field, value_col
        )

    return response


# ---------------------------------------------------------------------------
# Chat — SSE streaming
# ---------------------------------------------------------------------------

@router.post("/chat")
async def chat(request: ChatRequest, db: AsyncSession = Depends(get_db)):
    """Stream an AI chat response as Server-Sent Events.

    Content-Type: text/event-stream

    Event shapes (JSON ``data:`` lines):

    - ``{"type": "text_delta", "text": "..."}``
    - ``{"type": "tool_executing", "tool": "query_data", "input": {...}}``
    - ``{"type": "tool_result", "tool": "query_data", "result": {...}}``
    - ``{"type": "scenario_rule", "rule": {...}}``
    - ``{"type": "done"}``
    - ``{"type": "error", "message": "..."}``
    """
    from app.services.chat import stream_chat
    from app.services.ai_context import build_agent_context

    result = await db.execute(
        select(Dataset)
        .where(Dataset.id == request.dataset_id, Dataset.status != "deleted")
        .options(selectinload(Dataset.columns))
    )
    dataset = result.scalar_one_or_none()
    if dataset is None:
        raise HTTPException(status_code=404, detail="Dataset not found")

    if not settings.ANTHROPIC_API_KEY_CHAT:
        raise HTTPException(status_code=503, detail="ANTHROPIC_API_KEY_CHAT is not configured")

    # Load all related dataset IDs (fact + any joined dimensions via relationships)
    all_rel_result = await db.execute(
        select(DatasetRelationship).where(
            (DatasetRelationship.source_dataset_id == dataset.id) |
            (DatasetRelationship.target_dataset_id == dataset.id)
        )
    )
    all_rels = all_rel_result.scalars().all()
    related_ids = {dataset.id}
    for r in all_rels:
        related_ids.add(r.source_dataset_id)
        related_ids.add(r.target_dataset_id)

    context = await build_agent_context(list(related_ids), db)

    # Build enriched baseline so chat tools can see dimension-joined columns
    all_rel_refs = [RelationshipRef(rel_id=r.id) for r in all_rels]
    try:
        baseline_df = await _build_baseline_df(dataset, all_rel_refs, db)
    except Exception as exc:
        logger.warning("Failed to build baseline for chat tools: %s", exc)
        baseline_df = None

    return StreamingResponse(
        stream_chat(
            message=request.message,
            dataset_id=request.dataset_id,
            history=request.conversation_history,
            context=context,
            baseline_df=baseline_df,
            agent_mode=request.agent_mode,
        ),
        media_type="text/event-stream",
    )


# ---------------------------------------------------------------------------
# Semantic layer endpoints
# ---------------------------------------------------------------------------

@router.get("/datasets/{dataset_id}/semantic", response_model=SemanticLayerResponse)
async def get_semantic_layer(dataset_id: str, db: AsyncSession = Depends(get_db)):
    """Return the full semantic layer for a dataset."""
    result = await db.execute(
        select(Dataset)
        .where(Dataset.id == dataset_id, Dataset.status != "deleted")
    )
    dataset = result.scalar_one_or_none()
    if dataset is None:
        raise HTTPException(status_code=404, detail="Dataset not found")

    sc_result = await db.execute(
        select(SemanticColumn)
        .where(SemanticColumn.dataset_id == dataset_id)
        .options(selectinload(SemanticColumn.labels))
    )
    sem_cols = sc_result.scalars().all()

    return SemanticLayerResponse(
        dataset_id=dataset_id,
        columns=[SemanticColumnResponse.model_validate(c) for c in sem_cols],
        agent_context_notes=dataset.agent_context_notes,
    )


@router.put("/datasets/{dataset_id}/semantic/columns/{column_name}", response_model=SemanticColumnResponse)
async def update_semantic_column(
    dataset_id: str,
    column_name: str,
    body: SemanticColumnUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Update a semantic column's description and/or synonyms."""
    result = await db.execute(
        select(SemanticColumn)
        .where(SemanticColumn.dataset_id == dataset_id, SemanticColumn.column_name == column_name)
        .options(selectinload(SemanticColumn.labels))
    )
    sem_col = result.scalar_one_or_none()
    if sem_col is None:
        # Create it
        sem_col = SemanticColumn(
            id=uuid.uuid4().hex,
            dataset_id=dataset_id,
            column_name=column_name,
            description=body.description,
            synonyms=body.synonyms or [],
        )
        db.add(sem_col)
    else:
        if body.description is not None:
            sem_col.description = body.description
        if body.synonyms is not None:
            sem_col.synonyms = body.synonyms
    await db.commit()
    await db.refresh(sem_col)
    return SemanticColumnResponse.model_validate(sem_col)


@router.post("/datasets/{dataset_id}/semantic/labels", response_model=SemanticColumnResponse)
async def bulk_upsert_labels(
    dataset_id: str,
    body: SemanticLabelBulkCreate,
    db: AsyncSession = Depends(get_db),
):
    """Bulk upsert value labels for a column."""
    result = await db.execute(
        select(SemanticColumn)
        .where(SemanticColumn.dataset_id == dataset_id, SemanticColumn.column_name == body.column_name)
        .options(selectinload(SemanticColumn.labels))
    )
    sem_col = result.scalar_one_or_none()
    if sem_col is None:
        sem_col = SemanticColumn(
            id=uuid.uuid4().hex,
            dataset_id=dataset_id,
            column_name=body.column_name,
            synonyms=[],
        )
        db.add(sem_col)
        await db.flush()

    for lbl in body.labels:
        raw_str = str(lbl.get("raw_value", ""))
        if not raw_str:
            continue
        existing_lbl = await db.execute(
            select(SemanticValueLabel).where(
                SemanticValueLabel.semantic_column_id == sem_col.id,
                SemanticValueLabel.raw_value == raw_str,
            )
        )
        existing = existing_lbl.scalar_one_or_none()
        if existing:
            existing.display_label = str(lbl.get("display_label", raw_str))
            if lbl.get("category") is not None:
                existing.category = lbl["category"]
        else:
            db.add(SemanticValueLabel(
                id=uuid.uuid4().hex,
                semantic_column_id=sem_col.id,
                raw_value=raw_str,
                display_label=str(lbl.get("display_label", raw_str)),
                category=lbl.get("category"),
            ))

    await db.commit()
    await db.refresh(sem_col)
    return SemanticColumnResponse.model_validate(sem_col)


@router.delete("/datasets/{dataset_id}/semantic/labels/{label_id}", status_code=204)
async def delete_semantic_label(
    dataset_id: str,
    label_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Delete a single value label."""
    result = await db.execute(
        select(SemanticValueLabel)
        .join(SemanticColumn, SemanticValueLabel.semantic_column_id == SemanticColumn.id)
        .where(SemanticValueLabel.id == label_id, SemanticColumn.dataset_id == dataset_id)
    )
    lbl = result.scalar_one_or_none()
    if lbl is None:
        raise HTTPException(status_code=404, detail="Label not found")
    await db.delete(lbl)
    await db.commit()


# ---------------------------------------------------------------------------
# Transformation endpoints
# ---------------------------------------------------------------------------

async def _get_dataset_or_404(dataset_id: str, db: AsyncSession) -> Dataset:
    result = await db.execute(
        select(Dataset)
        .where(Dataset.id == dataset_id, Dataset.status != "deleted")
        .options(selectinload(Dataset.columns))
    )
    ds = result.scalar_one_or_none()
    if ds is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return ds


@router.get("/datasets/{dataset_id}/transformations", response_model=list[TransformationStepResponse])
async def list_transformations(dataset_id: str, db: AsyncSession = Depends(get_db)):
    """List all transformation steps for a dataset (audit trail)."""
    await _get_dataset_or_404(dataset_id, db)
    result = await db.execute(
        select(TransformationStep)
        .where(TransformationStep.dataset_id == dataset_id)
        .order_by(TransformationStep.step_order)
    )
    return [TransformationStepResponse.model_validate(s) for s in result.scalars().all()]


@router.post("/datasets/{dataset_id}/transformations/preview", response_model=TransformationPreviewResponse)
async def preview_transformation(
    dataset_id: str,
    body: TransformationStepCreate,
    db: AsyncSession = Depends(get_db),
):
    """Preview a transformation step on sample data without saving anything."""
    from app.services import transform as transform_svc

    ds = await _get_dataset_or_404(dataset_id, db)
    defn = dict(body.definition)
    if "step_type" not in defn:
        defn["step_type"] = body.step_type

    try:
        df = storage_svc.read_dataset(sync_engine, ds.table_name, limit=200)
        preview = transform_svc.preview_step(df, defn)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Preview failed: {exc}") from exc

    return TransformationPreviewResponse(step=None, preview=preview)


@router.post("/datasets/{dataset_id}/transformations", response_model=TransformationStepResponse, status_code=201)
async def create_transformation(
    dataset_id: str,
    body: TransformationStepCreate,
    db: AsyncSession = Depends(get_db),
):
    """Create a pending transformation step."""
    await _get_dataset_or_404(dataset_id, db)

    # Determine next step_order
    result = await db.execute(
        select(TransformationStep)
        .where(TransformationStep.dataset_id == dataset_id)
        .order_by(TransformationStep.step_order.desc())
    )
    steps = result.scalars().all()
    next_order = (steps[0].step_order + 1) if steps else 0

    defn = dict(body.definition)
    if "step_type" not in defn:
        defn["step_type"] = body.step_type

    step = TransformationStep(
        id=uuid.uuid4().hex,
        dataset_id=dataset_id,
        step_order=next_order,
        step_type=body.step_type,
        name=body.name,
        description=body.description,
        definition=defn,
        status="pending",
        created_by="user",
        ai_prompt=body.ai_prompt,
    )
    db.add(step)
    await db.commit()
    await db.refresh(step)
    return TransformationStepResponse.model_validate(step)


@router.post("/datasets/{dataset_id}/transformations/{step_id}/approve", response_model=TransformationStepResponse)
async def approve_transformation(
    dataset_id: str,
    step_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Approve and apply a transformation — materializes the column into PostgreSQL."""
    from app.services import transform as transform_svc

    ds = await _get_dataset_or_404(dataset_id, db)

    result = await db.execute(
        select(TransformationStep).where(
            TransformationStep.id == step_id,
            TransformationStep.dataset_id == dataset_id,
        )
    )
    step = result.scalar_one_or_none()
    if step is None:
        raise HTTPException(status_code=404, detail="Transformation step not found")
    if step.status == "applied":
        raise HTTPException(status_code=409, detail="Step is already applied")
    if step.status == "rejected":
        raise HTTPException(status_code=409, detail="Cannot apply a rejected step")

    # Materialize
    try:
        transform_svc.materialize_step(sync_engine, ds.table_name, step.definition)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Materialization failed: {exc}") from exc

    step.status = "applied"

    # Update DatasetColumn catalog: add/rename column entry
    step_type = step.step_type
    if step_type == "rename":
        from_col = step.definition["from_column"]
        to_col = step.definition["to_column"]
        col_result = await db.execute(
            select(DatasetColumn).where(
                DatasetColumn.dataset_id == dataset_id,
                DatasetColumn.column_name == from_col,
            )
        )
        col = col_result.scalar_one_or_none()
        if col:
            col.column_name = to_col
            col.display_name = to_col
    else:
        output_col = step.definition.get("output_column")
        if output_col:
            existing_col = await db.execute(
                select(DatasetColumn).where(
                    DatasetColumn.dataset_id == dataset_id,
                    DatasetColumn.column_name == output_col,
                )
            )
            if not existing_col.scalar_one_or_none():
                output_type = step.definition.get("output_type", "text")
                db.add(DatasetColumn(
                    id=uuid.uuid4().hex,
                    dataset_id=dataset_id,
                    column_name=output_col,
                    display_name=step.name,
                    data_type=output_type,
                    column_role="attribute" if output_type == "text" else "measure",
                    unique_count=None,
                    sample_values=None,
                ))

            # Create SemanticColumn entry for the new column
            existing_sc = await db.execute(
                select(SemanticColumn).where(
                    SemanticColumn.dataset_id == dataset_id,
                    SemanticColumn.column_name == output_col,
                )
            )
            if not existing_sc.scalar_one_or_none():
                db.add(SemanticColumn(
                    id=uuid.uuid4().hex,
                    dataset_id=dataset_id,
                    column_name=output_col,
                    description=step.description or f"Calculated column: {step.name}",
                    synonyms=[],
                ))

    await db.commit()
    await db.refresh(step)
    return TransformationStepResponse.model_validate(step)


@router.post("/datasets/{dataset_id}/transformations/{step_id}/reject", response_model=TransformationStepResponse)
async def reject_transformation(
    dataset_id: str,
    step_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Reject a pending transformation step."""
    result = await db.execute(
        select(TransformationStep).where(
            TransformationStep.id == step_id,
            TransformationStep.dataset_id == dataset_id,
        )
    )
    step = result.scalar_one_or_none()
    if step is None:
        raise HTTPException(status_code=404, detail="Transformation step not found")
    if step.status == "applied":
        raise HTTPException(status_code=409, detail="Cannot reject an already applied step")
    step.status = "rejected"
    await db.commit()
    await db.refresh(step)
    return TransformationStepResponse.model_validate(step)


@router.delete("/datasets/{dataset_id}/transformations/{step_id}", status_code=204)
async def delete_transformation(
    dataset_id: str,
    step_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Delete a pending or rejected transformation step. Applied steps cannot be deleted."""
    result = await db.execute(
        select(TransformationStep).where(
            TransformationStep.id == step_id,
            TransformationStep.dataset_id == dataset_id,
        )
    )
    step = result.scalar_one_or_none()
    if step is None:
        raise HTTPException(status_code=404, detail="Transformation step not found")
    if step.status == "applied":
        raise HTTPException(status_code=409, detail="Cannot delete an applied transformation step")
    await db.delete(step)
    await db.commit()


@router.post("/datasets/{dataset_id}/transformations/suggest", response_model=TransformationPreviewResponse)
async def suggest_transformation(
    dataset_id: str,
    body: TransformationSuggestRequest,
    db: AsyncSession = Depends(get_db),
):
    """AI suggests a transformation definition from a natural language prompt.

    Uses Claude with the semantic context to produce a structured JSON definition —
    no arbitrary code is generated or executed.
    The returned step has status='pending'; the user must call /approve to apply it.
    """
    from app.services import transform as transform_svc
    from app.services.ai_context import build_agent_context

    if not settings.ANTHROPIC_API_KEY_CHAT:
        raise HTTPException(status_code=503, detail="ANTHROPIC_API_KEY_CHAT is not configured")

    ds = await _get_dataset_or_404(dataset_id, db)

    # Load semantic context
    context = await build_agent_context([dataset_id], db)

    # Build column info for the prompt
    col_info = [
        {"name": c.column_name, "display": c.display_name, "type": c.data_type, "role": c.column_role}
        for c in ds.columns
    ]

    import anthropic
    client = anthropic.AsyncAnthropic(api_key=settings.ANTHROPIC_API_KEY_CHAT)

    # Tool schema for the transformation definition
    transform_tool_schema = {
        "type": "object",
        "required": ["name", "step_type", "definition", "reasoning"],
        "properties": {
            "name": {"type": "string", "description": "Human-readable name for this transformation"},
            "description": {"type": "string"},
            "step_type": {
                "type": "string",
                "enum": ["reclassification", "calculated_column", "concat"],
            },
            "definition": {
                "type": "object",
                "description": "Complete transformation definition JSON — must include step_type, output_column, and all required fields for the step type",
            },
            "reasoning": {"type": "string", "description": "Why this transformation is useful"},
        },
    }

    system_prompt = (
        "You are a data transformation assistant. Given a user's request and dataset context, "
        "produce a structured transformation definition.\n\n"
        "IMPORTANT: Only output declarative JSON rules — never suggest executable code.\n\n"
        f"{context}\n\n"
        "Available columns:\n" +
        "\n".join(f"  - {c['name']} ({c['type']}, role={c['role']})" for c in col_info) +
        "\n\nFor reclassification steps:\n"
        "  - source_column: column to read from\n"
        "  - output_column: name for the new column\n"
        "  - output_type: 'text'\n"
        "  - rules: array of {condition: {op, values/value}, result} ending with {default: 'Other'}\n"
        "  - Supported ops: between, in, equals, contains, starts_with\n"
        "For calculated_column steps:\n"
        "  - output_column, output_type: 'numeric'\n"
        "  - expression: nested {op, left, right} tree; leaves are {column: name} or {literal: 0}\n"
        "  - Supported ops: add, subtract, multiply, divide, abs, negate, round\n"
        "For concat steps:\n"
        "  - columns: list of column names, output_column, separator\n"
    )

    try:
        response = await client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            system=system_prompt,
            messages=[{"role": "user", "content": body.prompt}],
            tools=[{
                "name": "create_transformation",
                "description": "Submit the transformation definition",
                "input_schema": transform_tool_schema,
            }],
            tool_choice={"type": "any"},
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"AI suggestion failed: {exc}") from exc

    # Extract tool result
    suggestion = None
    for block in response.content:
        if hasattr(block, "type") and block.type == "tool_use" and block.name == "create_transformation":
            suggestion = block.input
            break

    if not suggestion:
        raise HTTPException(status_code=502, detail="AI did not return a valid transformation definition")

    defn = dict(suggestion.get("definition", {}))
    if "step_type" not in defn:
        defn["step_type"] = suggestion.get("step_type", "reclassification")

    # Validate and preview before saving
    try:
        df = storage_svc.read_dataset(sync_engine, ds.table_name, limit=200)
        preview = transform_svc.preview_step(df, defn)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Preview of AI suggestion failed: {exc}") from exc

    # Persist as pending step
    existing = await db.execute(
        select(TransformationStep).where(TransformationStep.dataset_id == dataset_id)
    )
    existing_steps = existing.scalars().all()
    next_order = (max(s.step_order for s in existing_steps) + 1) if existing_steps else 0

    step = TransformationStep(
        id=uuid.uuid4().hex,
        dataset_id=dataset_id,
        step_order=next_order,
        step_type=suggestion.get("step_type", "reclassification"),
        name=suggestion.get("name", "AI Suggestion"),
        description=suggestion.get("description"),
        definition=defn,
        status="pending",
        created_by="ai_agent",
        ai_prompt=body.prompt,
    )
    db.add(step)
    await db.commit()
    await db.refresh(step)

    return TransformationPreviewResponse(
        step=TransformationStepResponse.model_validate(step),
        preview=preview,
    )


# ---------------------------------------------------------------------------
# Knowledge entries
# ---------------------------------------------------------------------------

@router.get("/datasets/{dataset_id}/knowledge", response_model=list[KnowledgeEntryResponse])
async def list_knowledge(dataset_id: str, db: AsyncSession = Depends(get_db)):
    """List all knowledge entries for a dataset."""
    result = await db.execute(
        select(KnowledgeEntry)
        .where(KnowledgeEntry.dataset_id == dataset_id)
        .order_by(KnowledgeEntry.created_at.desc())
    )
    return [KnowledgeEntryResponse.model_validate(e) for e in result.scalars().all()]


@router.post("/datasets/{dataset_id}/knowledge", response_model=KnowledgeEntryResponse, status_code=201)
async def create_knowledge(
    dataset_id: str,
    body: KnowledgeEntryCreate,
    db: AsyncSession = Depends(get_db),
):
    """Create a new knowledge entry for a dataset."""
    r = await db.execute(select(Dataset).where(Dataset.id == dataset_id, Dataset.status != "deleted"))
    if r.scalar_one_or_none() is None:
        raise HTTPException(status_code=404, detail="Dataset not found")

    entry = KnowledgeEntry(
        id=uuid.uuid4().hex,
        dataset_id=dataset_id,
        entry_type=body.entry_type,
        plain_text=body.plain_text,
        content=body.content,
        confidence=body.confidence,
        source=body.source,
    )
    db.add(entry)
    await db.commit()
    await db.refresh(entry)
    return KnowledgeEntryResponse.model_validate(entry)


@router.put("/knowledge/{entry_id}", response_model=KnowledgeEntryResponse)
async def update_knowledge(
    entry_id: str,
    body: KnowledgeEntryUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Update a knowledge entry."""
    result = await db.execute(select(KnowledgeEntry).where(KnowledgeEntry.id == entry_id))
    entry = result.scalar_one_or_none()
    if entry is None:
        raise HTTPException(status_code=404, detail="Knowledge entry not found")

    if body.plain_text is not None:
        entry.plain_text = body.plain_text
    if body.content is not None:
        entry.content = body.content
    if body.confidence is not None:
        entry.confidence = body.confidence

    await db.commit()
    await db.refresh(entry)
    return KnowledgeEntryResponse.model_validate(entry)


@router.delete("/knowledge/{entry_id}", status_code=204)
async def delete_knowledge(entry_id: str, db: AsyncSession = Depends(get_db)):
    """Delete a knowledge entry."""
    result = await db.execute(select(KnowledgeEntry).where(KnowledgeEntry.id == entry_id))
    entry = result.scalar_one_or_none()
    if entry is None:
        raise HTTPException(status_code=404, detail="Knowledge entry not found")
    await db.delete(entry)
    await db.commit()
