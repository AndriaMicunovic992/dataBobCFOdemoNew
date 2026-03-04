"""All API endpoints for DataBobIQ."""

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
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

router = APIRouter()


# --- File upload ---

@router.post("/upload", response_model=DatasetResponse)
async def upload_file(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    """Upload a CSV or Excel file, parse schema, create dynamic table, load data.

    TODO: Implement this endpoint.
    - Save the file to UPLOAD_DIR
    - Call parser.parse_file()
    - Call storage.create_dataset_table() and storage.load_data()
    - Persist Dataset + DatasetColumn records
    - Return DatasetResponse
    """
    raise HTTPException(status_code=501, detail="Not implemented yet")


# --- Datasets ---

@router.get("/datasets", response_model=list[DatasetResponse])
async def list_datasets(db: AsyncSession = Depends(get_db)):
    """List all datasets.

    TODO: Query datasets table, eager-load columns.
    """
    raise HTTPException(status_code=501, detail="Not implemented yet")


@router.get("/datasets/{dataset_id}", response_model=SchemaResponse)
async def get_dataset(dataset_id: str, db: AsyncSession = Depends(get_db)):
    """Get dataset with columns and relationships.

    TODO: Fetch dataset or return 404.
    """
    raise HTTPException(status_code=501, detail="Not implemented yet")


@router.delete("/datasets/{dataset_id}", status_code=204)
async def delete_dataset(dataset_id: str, db: AsyncSession = Depends(get_db)):
    """Delete a dataset and drop its dynamic table.

    TODO: Drop the dynamic table, delete metadata records.
    """
    raise HTTPException(status_code=501, detail="Not implemented yet")


@router.patch("/datasets/{dataset_id}/columns/{column_id}", response_model=None)
async def update_column(
    dataset_id: str,
    column_id: str,
    body: DatasetColumnUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Update column metadata (role, display name).

    TODO: Fetch column, apply changes, commit.
    """
    raise HTTPException(status_code=501, detail="Not implemented yet")


# --- Relationships ---

@router.post("/relationships", response_model=DatasetRelationshipResponse)
async def create_relationship(
    body: DatasetRelationshipCreate,
    db: AsyncSession = Depends(get_db),
):
    """Create a relationship between two datasets.

    TODO: Persist relationship record.
    """
    raise HTTPException(status_code=501, detail="Not implemented yet")


# --- Scenarios ---

@router.get("/scenarios", response_model=list[ScenarioResponse])
async def list_scenarios(db: AsyncSession = Depends(get_db)):
    """List all scenarios.

    TODO: Query scenarios table.
    """
    raise HTTPException(status_code=501, detail="Not implemented yet")


@router.post("/scenarios", response_model=ScenarioResponse)
async def create_scenario(body: ScenarioCreate, db: AsyncSession = Depends(get_db)):
    """Create a new scenario.

    TODO: Persist and return.
    """
    raise HTTPException(status_code=501, detail="Not implemented yet")


@router.patch("/scenarios/{scenario_id}", response_model=ScenarioResponse)
async def update_scenario(
    scenario_id: str,
    body: ScenarioUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Update a scenario's name, rules, or color.

    TODO: Fetch, patch, commit.
    """
    raise HTTPException(status_code=501, detail="Not implemented yet")


@router.delete("/scenarios/{scenario_id}", status_code=204)
async def delete_scenario(scenario_id: str, db: AsyncSession = Depends(get_db)):
    """Delete a scenario.

    TODO: Delete record.
    """
    raise HTTPException(status_code=501, detail="Not implemented yet")


# --- Query ---

@router.post("/query", response_model=QueryResponse)
async def query_dataset(request: QueryRequest, db: AsyncSession = Depends(get_db)):
    """Execute a dynamic query with optional filters, grouping, and pivot.

    TODO: Delegate to query.execute_query().
    """
    raise HTTPException(status_code=501, detail="Not implemented yet")


# --- Chat ---

@router.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest, db: AsyncSession = Depends(get_db)):
    """Send a message to Claude AI with dataset context.

    TODO: Delegate to chat.chat_with_data().
    """
    raise HTTPException(status_code=501, detail="Not implemented yet")
