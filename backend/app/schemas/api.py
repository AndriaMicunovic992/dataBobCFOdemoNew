"""Pydantic request/response schemas for the API."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Model (top-level workspace container)
# ---------------------------------------------------------------------------

class ModelCreate(BaseModel):
    name: str
    description: str | None = None


class ModelUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    status: str | None = None  # active | archived
    settings: dict[str, Any] | None = None


class ModelSummary(BaseModel):
    id: str
    name: str
    description: str | None = None
    status: str
    dataset_count: int = 0
    scenario_count: int = 0
    created_at: datetime
    updated_at: datetime | None = None
    settings: dict[str, Any] | None = None

    model_config = {"from_attributes": True}


class ModelResponse(BaseModel):
    id: str
    name: str
    description: str | None = None
    status: str
    created_at: datetime
    updated_at: datetime | None = None
    settings: dict[str, Any] | None = None

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# Dataset column
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------

class DatasetResponse(BaseModel):
    id: str
    model_id: str | None = None
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


class DatasetRelationshipUpdate(BaseModel):
    source_column: str | None = None
    target_column: str | None = None


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
# Query
# ---------------------------------------------------------------------------

class OrderByClause(BaseModel):
    column: str
    direction: Literal["asc", "desc"] = "asc"


class QueryRequest(BaseModel):
    """Flexible query against a dynamic dataset table.

    ``filters`` values can be a single value (equality) or a list (IN clause).
    ``aggregations`` maps column name -> agg function (sum, avg, min, max, count).
    """
    columns: list[str] | None = None
    filters: dict[str, Any] | None = None
    group_by: list[str] | None = None
    aggregations: dict[str, str] | None = None
    order_by: list[OrderByClause] | None = None
    limit: int = Field(default=1000, le=10_000)


class QueryResponse(BaseModel):
    columns: list[str]
    data: list[list[Any]]
    total_rows: int


# ---------------------------------------------------------------------------
# Baseline
# ---------------------------------------------------------------------------

class RelationshipRef(BaseModel):
    rel_id: str


class BaselineRequest(BaseModel):
    fact_dataset_id: str
    relationships: list[RelationshipRef] = []


class BaselineResponse(BaseModel):
    columns: list[str]
    data: list[list[Any]]
    row_count: int


# ---------------------------------------------------------------------------
# Scenario
# ---------------------------------------------------------------------------

class ScenarioBaseConfig(BaseModel):
    """Baseline configuration — what data the scenario starts from."""
    model_config = {"extra": "ignore"}

    source: Literal["actuals", "scenario"] = "actuals"
    source_scenario_id: str | None = None   # when source="scenario"
    base_year: int | None = None            # e.g. 2025 → Jan-Dec of that year
    # Legacy fields kept for backwards compatibility with stored data
    period_from: str | None = None
    period_to: str | None = None


class ScenarioCreate(BaseModel):
    name: str
    dataset_id: str
    rules: list[dict[str, Any]] = []
    color: str | None = None
    base_config: ScenarioBaseConfig | None = None


class ScenarioUpdate(BaseModel):
    name: str | None = None
    rules: list[dict[str, Any]] | None = None
    color: str | None = None
    base_config: ScenarioBaseConfig | None = None


class ScenarioResponse(BaseModel):
    id: str
    model_id: str | None = None
    name: str
    dataset_id: str
    rules: list[dict[str, Any]]
    color: str | None = None
    base_config: dict[str, Any] | None = None
    created_at: datetime
    updated_at: datetime | None = None

    model_config = {"from_attributes": True}


class ScenarioComputeRequest(BaseModel):
    fact_dataset_id: str
    relationships: list[RelationshipRef] = []
    value_column: str | None = None          # defaults to "amount" (auto-detected if absent)
    group_by: list[str] | None = None        # columns for variance grouping
    breakdown_field: str | None = None       # column for waterfall breakdown


# ---------------------------------------------------------------------------
# Chat
# ---------------------------------------------------------------------------

class ChatRequest(BaseModel):
    dataset_id: str
    message: str
    conversation_history: list[dict[str, str]] = []
    agent_mode: str = "scenario"  # "data_understanding" | "scenario"


class ChatResponse(BaseModel):
    message: str
    suggested_query: QueryRequest | None = None


# ---------------------------------------------------------------------------
# Semantic layer
# ---------------------------------------------------------------------------

class SemanticValueLabelResponse(BaseModel):
    id: str
    raw_value: str
    display_label: str
    category: str | None = None
    sort_order: int | None = None

    model_config = {"from_attributes": True}


class SemanticColumnResponse(BaseModel):
    id: str
    dataset_id: str
    column_name: str
    description: str | None = None
    synonyms: list[str] = []
    value_source: str | None = None
    labels: list[SemanticValueLabelResponse] = []

    model_config = {"from_attributes": True}


class SemanticLayerResponse(BaseModel):
    dataset_id: str
    columns: list[SemanticColumnResponse]
    agent_context_notes: dict[str, Any] | None = None


class SemanticColumnUpdate(BaseModel):
    description: str | None = None
    synonyms: list[str] | None = None


class SemanticLabelBulkCreate(BaseModel):
    column_name: str
    labels: list[dict[str, Any]]  # [{raw_value, display_label, category?}]


# ---------------------------------------------------------------------------
# Transformations
# ---------------------------------------------------------------------------

class TransformationStepCreate(BaseModel):
    step_type: str  # reclassification | calculated_column | rename | concat
    name: str
    description: str | None = None
    definition: dict[str, Any]
    ai_prompt: str | None = None


class TransformationStepResponse(BaseModel):
    id: str
    dataset_id: str
    step_order: int
    step_type: str
    name: str
    description: str | None = None
    definition: dict[str, Any]
    status: str
    created_by: str
    ai_prompt: str | None = None
    created_at: datetime
    updated_at: datetime | None = None

    model_config = {"from_attributes": True}


class TransformationPreviewResponse(BaseModel):
    step: TransformationStepResponse | None = None
    preview: dict[str, Any]


class TransformationSuggestRequest(BaseModel):
    prompt: str


# ---------------------------------------------------------------------------
# Knowledge entries
# ---------------------------------------------------------------------------

class KnowledgeEntryCreate(BaseModel):
    entry_type: str
    plain_text: str
    content: dict[str, Any] = {}
    confidence: str | None = None  # high | medium | low
    source: str = "user"


class KnowledgeEntryUpdate(BaseModel):
    plain_text: str | None = None
    content: dict[str, Any] | None = None
    confidence: str | None = None


class KnowledgeEntryResponse(BaseModel):
    id: str
    dataset_id: str
    entry_type: str
    plain_text: str
    content: dict[str, Any]
    confidence: str | None = None
    source: str
    created_at: datetime
    updated_at: datetime | None = None

    model_config = {"from_attributes": True}
