"""All API endpoints for DataBobIQ."""

from __future__ import annotations

import asyncio
import logging
import os
import uuid
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, UploadFile, File
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import settings
from app.database import get_db, sync_engine
from app.models.metadata import Dataset, DatasetColumn, DatasetRelationship
from app.schemas.api import (
    DatasetResponse,
    DatasetColumnUpdate,
    DatasetRelationshipCreate,
    DatasetRelationshipResponse,
    SchemaResponse,
    ScenarioCreate,
    ScenarioUpdate,
    ScenarioResponse,
    QueryRequest,
    QueryResponse,
    ChatRequest,
    ChatResponse,
)
from app.services import parser as parser_svc
from app.services import storage as storage_svc
from app.services import schema_agent

logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _run_agent_and_persist(dataset_id: str) -> None:
    """
    Background task: run the schema agent and persist results.

    If the agent call fails or times out the dataset stays with
    ai_analyzed=False — the reanalyze endpoint can retry later.
    """
    from app.database import AsyncSessionLocal  # avoid circular at module level

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
                "preview_rows": [],  # not stored yet; agent works from column stats
            }
        ]

        try:
            agent_result = await asyncio.wait_for(
                schema_agent.analyze_schema(tables_payload),
                timeout=15,
            )
        except asyncio.TimeoutError:
            logger.warning("Schema agent timed out for dataset %s", dataset_id)
            return
        except Exception as exc:
            logger.warning("Schema agent failed for dataset %s: %s", dataset_id, exc)
            return

        # Merge into dataset and columns
        agent_tables = {t["name"]: t for t in agent_result.get("tables", [])}
        agent_table = agent_tables.get(dataset.name, {})

        _, ai_notes = schema_agent.merge_agent_results([], agent_table)
        ai_notes["relationships"] = agent_result.get("suggested_relationships", [])
        ai_notes["warnings"] = agent_result.get("warnings", [])

        dataset.ai_notes = ai_notes
        dataset.ai_analyzed = True

        agent_col_map = {
            c["column_name"]: c for c in agent_table.get("columns", [])
        }
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
# File upload
# ---------------------------------------------------------------------------

@router.post("/upload", response_model=DatasetResponse)
async def upload_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    """
    Upload a CSV or Excel file, parse schema, create dynamic table, load data.

    Flow:
    1. Save file to UPLOAD_DIR
    2. parse_file() → heuristic type + role detection
    3. create_dataset_table() + load_data() (sync, in one transaction)
    4. Persist Dataset + DatasetColumn metadata rows
    5. Fire background task: schema agent (15-s timeout; non-blocking)
    6. Return DatasetResponse immediately with ai_analyzed=False
    """
    upload_dir = Path(settings.UPLOAD_DIR)
    upload_dir.mkdir(parents=True, exist_ok=True)

    safe_name = f"{uuid.uuid4().hex}_{file.filename}"
    file_path = upload_dir / safe_name
    content = await file.read()
    file_path.write_bytes(content)

    # --- Parse ---
    try:
        parsed_sheets = parser_svc.parse_file(str(file_path))
    except Exception as exc:
        file_path.unlink(missing_ok=True)
        raise HTTPException(status_code=422, detail=f"Failed to parse file: {exc}") from exc

    if not parsed_sheets:
        file_path.unlink(missing_ok=True)
        raise HTTPException(status_code=422, detail="No sheets/tables found in the file")

    # Use only the first sheet for now (multi-sheet upload can be a later step)
    sheet = parsed_sheets[0]

    table_name = storage_svc.generate_table_name()
    columns_meta = [
        {
            "column_name": col.column_name,
            "data_type": col.data_type,
            "column_role": col.column_role,
            "unique_count": col.unique_count,
            "display_name": col.display_name or col.column_name,
        }
        for col in sheet.columns
    ]

    # --- Create table + load data (sync, single transaction) ---
    try:
        storage_svc.create_dataset_table(sync_engine, table_name, columns_meta)
        row_count = storage_svc.load_data(sync_engine, table_name, sheet.dataframe, columns_meta)
    except Exception as exc:
        storage_svc.drop_dataset_table(sync_engine, table_name)
        file_path.unlink(missing_ok=True)
        raise HTTPException(status_code=500, detail=f"Failed to load data: {exc}") from exc

    # --- Persist metadata ---
    dataset_name = sheet.sheet_name or Path(file.filename).stem
    dataset = Dataset(
        id=uuid.uuid4().hex,
        name=dataset_name,
        table_name=table_name,
        source_filename=file.filename,
        row_count=row_count,
        status="ready",
        ai_analyzed=False,
    )
    db.add(dataset)

    for col in sheet.columns:
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

    await db.commit()
    await db.refresh(dataset, ["columns"])

    # --- Fire agent in background (non-blocking) ---
    if settings.ANTHROPIC_API_KEY_AGENT:
        background_tasks.add_task(_run_agent_and_persist, dataset.id)

    return DatasetResponse.model_validate(dataset)


# ---------------------------------------------------------------------------
# Datasets
# ---------------------------------------------------------------------------

@router.get("/datasets", response_model=list[DatasetResponse])
async def list_datasets(db: AsyncSession = Depends(get_db)):
    """List all datasets with their columns."""
    result = await db.execute(
        select(Dataset).options(selectinload(Dataset.columns)).order_by(Dataset.created_at.desc())
    )
    return [DatasetResponse.model_validate(d) for d in result.scalars().all()]


@router.get("/datasets/{dataset_id}", response_model=SchemaResponse)
async def get_dataset(dataset_id: str, db: AsyncSession = Depends(get_db)):
    """Get dataset with columns and relationships."""
    result = await db.execute(
        select(Dataset)
        .where(Dataset.id == dataset_id)
        .options(
            selectinload(Dataset.columns),
            selectinload(Dataset.source_relationships),
            selectinload(Dataset.target_relationships),
        )
    )
    dataset = result.scalar_one_or_none()
    if dataset is None:
        raise HTTPException(status_code=404, detail="Dataset not found")

    rels = list(dataset.source_relationships) + list(dataset.target_relationships)
    return SchemaResponse(
        dataset=DatasetResponse.model_validate(dataset),
        columns=[DatasetResponse.model_validate(dataset).columns],
        relationships=[DatasetRelationshipResponse.model_validate(r) for r in rels],
    )


@router.delete("/datasets/{dataset_id}", status_code=204)
async def delete_dataset(dataset_id: str, db: AsyncSession = Depends(get_db)):
    """Delete a dataset and drop its dynamic table."""
    result = await db.execute(select(Dataset).where(Dataset.id == dataset_id))
    dataset = result.scalar_one_or_none()
    if dataset is None:
        raise HTTPException(status_code=404, detail="Dataset not found")

    storage_svc.drop_dataset_table(sync_engine, dataset.table_name)
    await db.delete(dataset)
    await db.commit()


@router.patch("/datasets/{dataset_id}/columns/{column_id}", response_model=None)
async def update_column(
    dataset_id: str,
    column_id: str,
    body: DatasetColumnUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Update column metadata (role, display name)."""
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
    return {"ok": True}


# ---------------------------------------------------------------------------
# AI re-analysis
# ---------------------------------------------------------------------------

@router.post("/datasets/{dataset_id}/reanalyze", response_model=DatasetResponse)
async def reanalyze_dataset(
    dataset_id: str,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    """
    Retry the schema-agent analysis for a dataset.

    Useful when the original background task timed out or the API key
    was not configured at upload time.
    Returns immediately with the current (unchanged) dataset state;
    the analysis runs in the background.
    """
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

@router.post("/relationships", response_model=DatasetRelationshipResponse)
async def create_relationship(
    body: DatasetRelationshipCreate,
    db: AsyncSession = Depends(get_db),
):
    """Create a relationship between two datasets."""
    rel = DatasetRelationship(
        id=uuid.uuid4().hex,
        **body.model_dump(),
    )
    db.add(rel)
    await db.commit()
    await db.refresh(rel)
    return DatasetRelationshipResponse.model_validate(rel)


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------

@router.get("/scenarios", response_model=list[ScenarioResponse])
async def list_scenarios(db: AsyncSession = Depends(get_db)):
    raise HTTPException(status_code=501, detail="Not implemented yet")


@router.post("/scenarios", response_model=ScenarioResponse)
async def create_scenario(body: ScenarioCreate, db: AsyncSession = Depends(get_db)):
    raise HTTPException(status_code=501, detail="Not implemented yet")


@router.patch("/scenarios/{scenario_id}", response_model=ScenarioResponse)
async def update_scenario(
    scenario_id: str,
    body: ScenarioUpdate,
    db: AsyncSession = Depends(get_db),
):
    raise HTTPException(status_code=501, detail="Not implemented yet")


@router.delete("/scenarios/{scenario_id}", status_code=204)
async def delete_scenario(scenario_id: str, db: AsyncSession = Depends(get_db)):
    raise HTTPException(status_code=501, detail="Not implemented yet")


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------

@router.post("/query", response_model=QueryResponse)
async def query_dataset(request: QueryRequest, db: AsyncSession = Depends(get_db)):
    raise HTTPException(status_code=501, detail="Not implemented yet")


# ---------------------------------------------------------------------------
# Chat
# ---------------------------------------------------------------------------

@router.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest, db: AsyncSession = Depends(get_db)):
    raise HTTPException(status_code=501, detail="Not implemented yet")
