"""Pydantic request/response schemas for the API."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel


# --- Upload / Dataset schemas ---

class DatasetColumnResponse(BaseModel):
    id: str
    name: str
    original_name: str
    dtype: str
    position: int
    sample_values: list[Any] | None = None
    description: str | None = None


class DatasetResponse(BaseModel):
    id: str
    upload_id: str
    name: str
    table_name: str
    sheet_name: str | None = None
    row_count: int
    columns: list[DatasetColumnResponse] = []
    created_at: datetime

    model_config = {"from_attributes": True}


class UploadResponse(BaseModel):
    id: str
    filename: str
    original_filename: str
    file_size: int
    content_type: str
    created_at: datetime
    datasets: list[DatasetResponse] = []

    model_config = {"from_attributes": True}


# --- Query schemas ---

class QueryRequest(BaseModel):
    dataset_id: str
    filters: dict[str, Any] | None = None
    group_by: list[str] | None = None
    aggregate: dict[str, str] | None = None  # column -> agg function
    pivot_on: str | None = None
    pivot_values: str | None = None
    limit: int = 1000


class QueryResponse(BaseModel):
    dataset_id: str
    columns: list[str]
    rows: list[list[Any]]
    total_rows: int


# --- Scenario schemas ---

class ScenarioRequest(BaseModel):
    dataset_id: str
    adjustments: dict[str, float]  # column -> percentage adjustment
    group_by: list[str] | None = None


class ScenarioResponse(BaseModel):
    dataset_id: str
    columns: list[str]
    original: list[list[Any]]
    scenario: list[list[Any]]


# --- Chat schemas ---

class ChatRequest(BaseModel):
    dataset_id: str
    message: str
    conversation_history: list[dict[str, str]] = []


class ChatResponse(BaseModel):
    message: str
    suggested_query: QueryRequest | None = None
