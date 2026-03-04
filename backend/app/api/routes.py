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
from app.models.metadata import Dataset, DatasetColumn, DatasetRelationship, Scenario
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
    OrderByClause,
    QueryRequest,
    QueryResponse,
    RelationshipRef,
    ScenarioComputeRequest,
    ScenarioCreate,
    ScenarioResponse,
    ScenarioUpdate,
    SchemaResponse,
)
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
    if isinstance(v, float) and math.isnan(v):
        return None
    if isinstance(v, (date, datetime)):
        return v.isoformat()
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
            df = df.join(dim_df, left_on=fact_col, right_on=dim_col, how="left")
        except Exception as exc:
            logger.warning("Join on %s.%s failed: %s", dim_dataset.name, dim_col, exc)

    return df




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

        tables_payload = [
            {
                "name": dataset.name,
                "filename": dataset.source_filename or "",
                "row_count": dataset.row_count,
                "columns": [
                    {
                        "column_name": c.column_name,
                        "data_type": c.data_type,
                        "column_role": c.column_role,
                        "unique_count": c.unique_count,
                        "null_count": 0,
                        "sample_values": c.sample_values or [],
                    }
                    for c in dataset.columns
                ],
                "preview_rows": [],
            }
        ]

        try:
            agent_result = await asyncio.wait_for(
                schema_agent.analyze_schema(tables_payload), timeout=15
            )
        except asyncio.TimeoutError:
            logger.warning("Schema agent timed out for dataset %s", dataset_id)
            return
        except Exception as exc:
            logger.warning("Schema agent failed for dataset %s: %s", dataset_id, exc)
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
        for rel in detected_rels:
            src_ds = ds_by_name.get(rel["source"])
            tgt_ds = ds_by_name.get(rel["target"])
            if src_ds and tgt_ds:
                db.add(
                    DatasetRelationship(
                        id=uuid.uuid4().hex,
                        source_dataset_id=src_ds.id,
                        target_dataset_id=tgt_ds.id,
                        source_column=rel["source_col"],
                        target_column=rel["target_col"],
                        coverage_pct=rel.get("coverage"),
                        overlap_count=rel.get("overlap"),
                    )
                )
        await db.commit()

    # Reload with columns relationship populated
    responses: list[DatasetResponse] = []
    for ds in created_datasets:
        result = await db.execute(
            select(Dataset)
            .where(Dataset.id == ds.id)
            .options(selectinload(Dataset.columns))
        )
        loaded = result.scalar_one()
        responses.append(DatasetResponse.model_validate(loaded))

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

    try:
        scenario_df = scenario_svc.apply_rules(baseline_df, scenario.rules, value_col)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Rule application failed: {exc}") from exc

    def _rows(df: pl.DataFrame) -> list[list]:
        return [[_json_safe(v) for v in row] for row in df.iter_rows()]

    response: dict = {
        "scenario_id": scenario_id,
        "value_column": value_col,
        "columns": scenario_df.columns,
        "baseline": _rows(baseline_df),
        "scenario": _rows(scenario_df),
        "row_count": len(scenario_df),
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
    Each event: ``data: <json>\\n\\n``
    Final event: ``data: [DONE]\\n\\n``
    """
    # Validate dataset exists
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

    from app.services.chat import chat_with_data

    async def event_stream() -> AsyncGenerator[str, None]:
        try:
            response = await chat_with_data(
                dataset_id=request.dataset_id,
                message=request.message,
                history=request.conversation_history,
            )
            payload = json.dumps({"message": response.get("message", ""), "done": True})
            yield f"data: {payload}\n\n"
        except NotImplementedError:
            payload = json.dumps({"error": "Chat service not yet implemented"})
            yield f"data: {payload}\n\n"
        except Exception as exc:
            payload = json.dumps({"error": str(exc)})
            yield f"data: {payload}\n\n"
        finally:
            yield "data: [DONE]\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")
