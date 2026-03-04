"""Pydantic request/response schemas for the API."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------

class DatasetCreate(BaseModel):
    name: str | None = None


class DatasetColumnResponse(BaseModel):
    id: str
    dataset_id: str
    column_name: str
    display_name: str
    data_type: str
    column_role: str
    unique_count: int | None = None
    sample_values: list[Any] | None = None
    ai_suggestion: dict[str, Any] | None = None

    model_config = {"from_attributes": True}


class DatasetColumnUpdate(BaseModel):
    column_role: str | None = None
    display_name: str | None = None


class DatasetResponse(BaseModel):
    id: str
    name: str
    table_name: str
    source_filename: str | None = None
    row_count: int
    status: str
    ai_analyzed: bool = False
    ai_notes: dict[str, Any] | None = None
    created_at: datetime
    updated_at: datetime | None = None
    columns: list[DatasetColumnResponse] = []

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# Relationships
# ---------------------------------------------------------------------------

class DatasetRelationshipCreate(BaseModel):
    source_dataset_id: str
    target_dataset_id: str
    source_column: str
    target_column: str


class DatasetRelationshipResponse(BaseModel):
    id: str
    source_dataset_id: str
    target_dataset_id: str
    source_column: str
    target_column: str
    coverage_pct: int | None = None
    overlap_count: int | None = None
    created_at: datetime

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# Schema (dataset + columns + relationships together)
# ---------------------------------------------------------------------------

class SchemaResponse(BaseModel):
    dataset: DatasetResponse
    columns: list[DatasetColumnResponse]
    relationships: list[DatasetRelationshipResponse] = []


# ---------------------------------------------------------------------------
# Scenario
# ---------------------------------------------------------------------------

class ScenarioCreate(BaseModel):
    name: str
    dataset_id: str
    rules: list[dict[str, Any]] = []
    color: str | None = None


class ScenarioUpdate(BaseModel):
    name: str | None = None
    rules: list[dict[str, Any]] | None = None
    color: str | None = None


class ScenarioResponse(BaseModel):
    id: str
    name: str
    dataset_id: str
    rules: list[dict[str, Any]]
    color: str | None = None
    created_at: datetime
    updated_at: datetime | None = None

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Chat
# ---------------------------------------------------------------------------

class ChatRequest(BaseModel):
    dataset_id: str
    message: str
    conversation_history: list[dict[str, str]] = []


class ChatResponse(BaseModel):
    message: str
    suggested_query: QueryRequest | None = None
