"""All API endpoints for DataBobIQ."""

import os
import uuid
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.schemas.api import (
    UploadResponse,
    DatasetResponse,
    QueryRequest,
    QueryResponse,
    ScenarioRequest,
    ScenarioResponse,
    ChatRequest,
    ChatResponse,
)

router = APIRouter()


# --- File upload ---

@router.post("/upload", response_model=UploadResponse)
async def upload_file(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    """Upload a CSV or Excel file and parse its schema.

    TODO: Implement this endpoint.
    - Save the file to UPLOAD_DIR
    - Call parser.parse_file()
    - For each ParsedSheet, call storage.create_dataset_table() and storage.load_data()
    - Persist Upload + Dataset + DatasetColumn records
    - Return UploadResponse
    """
    raise HTTPException(status_code=501, detail="Not implemented yet")


# --- Datasets ---

@router.get("/datasets", response_model=list[DatasetResponse])
async def list_datasets(db: AsyncSession = Depends(get_db)):
    """List all available datasets.

    TODO: Query the datasets table and return all records with their columns.
    """
    raise HTTPException(status_code=501, detail="Not implemented yet")


@router.get("/datasets/{dataset_id}", response_model=DatasetResponse)
async def get_dataset(dataset_id: str, db: AsyncSession = Depends(get_db)):
    """Get a single dataset by ID with its column metadata.

    TODO: Fetch the dataset or return 404.
    """
    raise HTTPException(status_code=501, detail="Not implemented yet")


@router.delete("/datasets/{dataset_id}", status_code=204)
async def delete_dataset(dataset_id: str, db: AsyncSession = Depends(get_db)):
    """Delete a dataset and its dynamic table.

    TODO: Drop the dynamic table, delete metadata records.
    """
    raise HTTPException(status_code=501, detail="Not implemented yet")


# --- Query ---

@router.post("/query", response_model=QueryResponse)
async def query_dataset(request: QueryRequest, db: AsyncSession = Depends(get_db)):
    """Execute a dynamic query with optional filters, grouping, and pivot.

    TODO: Delegate to query.execute_query().
    """
    raise HTTPException(status_code=501, detail="Not implemented yet")


# --- Scenario ---

@router.post("/scenario", response_model=ScenarioResponse)
async def run_scenario(request: ScenarioRequest, db: AsyncSession = Depends(get_db)):
    """Run a what-if scenario with percentage adjustments on numeric columns.

    TODO: Delegate to scenario.compute_scenario().
    """
    raise HTTPException(status_code=501, detail="Not implemented yet")


# --- Chat ---

@router.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest, db: AsyncSession = Depends(get_db)):
    """Send a message to Claude AI with dataset context.

    TODO: Delegate to chat.chat_with_data().
    """
    raise HTTPException(status_code=501, detail="Not implemented yet")
