"""SQLAlchemy models for dataset metadata catalog."""

import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean, String, DateTime, Integer, Text, ForeignKey, JSON, UniqueConstraint
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from app.database import Base


class Model(Base):
    """Top-level workspace container. All datasets, scenarios, and knowledge belong to a model."""

    __tablename__ = "models"

    id: Mapped[str] = mapped_column(
        String, primary_key=True, server_default=func.gen_random_uuid().cast(String)
    )
    name: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String, nullable=False, default="active")  # active | archived
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), onupdate=func.now(), nullable=True
    )

    datasets: Mapped[list["Dataset"]] = relationship(
        "Dataset", back_populates="model", cascade="all, delete-orphan"
    )
    scenarios: Mapped[list["Scenario"]] = relationship(
        "Scenario", back_populates="model", cascade="all, delete-orphan"
    )
    knowledge_entries: Mapped[list["KnowledgeEntry"]] = relationship(
        "KnowledgeEntry", back_populates="model", cascade="all, delete-orphan"
    )


class Dataset(Base):
    """Tracks a parsed dataset and its dynamic PostgreSQL table."""

    __tablename__ = "datasets"

    id: Mapped[str] = mapped_column(
        String, primary_key=True, server_default=func.gen_random_uuid().cast(String)
    )
    model_id: Mapped[str | None] = mapped_column(
        String, ForeignKey("models.id", ondelete="SET NULL"), nullable=True, index=True
    )
    name: Mapped[str] = mapped_column(String, nullable=False)
    table_name: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    source_filename: Mapped[str | None] = mapped_column(String, nullable=True)
    row_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    status: Mapped[str] = mapped_column(String, nullable=False, default="processing")
    # AI schema-agent fields
    ai_analyzed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    ai_notes: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), onupdate=func.now(), nullable=True
    )

    # Semantic layer: structured agent summary injected into scenario agent prompts
    agent_context_notes: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    model: Mapped["Model | None"] = relationship("Model", back_populates="datasets")
    columns: Mapped[list["DatasetColumn"]] = relationship(
        "DatasetColumn", back_populates="dataset", cascade="all, delete-orphan"
    )
    scenarios: Mapped[list["Scenario"]] = relationship(
        "Scenario", back_populates="dataset", cascade="all, delete-orphan"
    )
    semantic_columns: Mapped[list["SemanticColumn"]] = relationship(
        "SemanticColumn", back_populates="dataset", cascade="all, delete-orphan"
    )
    transformations: Mapped[list["TransformationStep"]] = relationship(
        "TransformationStep", back_populates="dataset", cascade="all, delete-orphan",
        order_by="TransformationStep.step_order",
    )
    knowledge_entries: Mapped[list["KnowledgeEntry"]] = relationship(
        "KnowledgeEntry", back_populates="dataset", cascade="all, delete-orphan",
    )
    source_relationships: Mapped[list["DatasetRelationship"]] = relationship(
        "DatasetRelationship",
        back_populates="source_dataset",
        foreign_keys="DatasetRelationship.source_dataset_id",
        cascade="all, delete-orphan",
    )
    target_relationships: Mapped[list["DatasetRelationship"]] = relationship(
        "DatasetRelationship",
        back_populates="target_dataset",
        foreign_keys="DatasetRelationship.target_dataset_id",
        cascade="all, delete-orphan",
    )


class DatasetColumn(Base):
    """Describes a column within a dataset's dynamic table."""

    __tablename__ = "dataset_columns"
    __table_args__ = (
        UniqueConstraint("dataset_id", "column_name", name="uq_dataset_column"),
    )

    id: Mapped[str] = mapped_column(
        String, primary_key=True, server_default=func.gen_random_uuid().cast(String)
    )
    dataset_id: Mapped[str] = mapped_column(
        String, ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False
    )
    column_name: Mapped[str] = mapped_column(String, nullable=False)
    display_name: Mapped[str] = mapped_column(String, nullable=False)
    data_type: Mapped[str] = mapped_column(String, nullable=False)  # text/numeric/integer/date/boolean
    column_role: Mapped[str] = mapped_column(String, nullable=False, default="attribute")
    unique_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    sample_values: Mapped[list | None] = mapped_column(JSON, nullable=True)
    # AI schema-agent reasoning for this column
    ai_suggestion: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    dataset: Mapped["Dataset"] = relationship("Dataset", back_populates="columns")


class DatasetRelationship(Base):
    """Detected or user-defined join relationship between two datasets."""

    __tablename__ = "dataset_relationships"

    id: Mapped[str] = mapped_column(
        String, primary_key=True, server_default=func.gen_random_uuid().cast(String)
    )
    model_id: Mapped[str | None] = mapped_column(
        String, ForeignKey("models.id", ondelete="CASCADE"), nullable=True, index=True
    )
    source_dataset_id: Mapped[str] = mapped_column(
        String, ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False
    )
    target_dataset_id: Mapped[str] = mapped_column(
        String, ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False
    )
    source_column: Mapped[str] = mapped_column(String, nullable=False)
    target_column: Mapped[str] = mapped_column(String, nullable=False)
    coverage_pct: Mapped[int | None] = mapped_column(Integer, nullable=True)
    overlap_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    source_dataset: Mapped["Dataset"] = relationship(
        "Dataset", back_populates="source_relationships", foreign_keys=[source_dataset_id]
    )
    target_dataset: Mapped["Dataset"] = relationship(
        "Dataset", back_populates="target_relationships", foreign_keys=[target_dataset_id]
    )


class Scenario(Base):
    """A saved what-if scenario with adjustment rules."""

    __tablename__ = "scenarios"

    id: Mapped[str] = mapped_column(
        String, primary_key=True, server_default=func.gen_random_uuid().cast(String)
    )
    model_id: Mapped[str | None] = mapped_column(
        String, ForeignKey("models.id", ondelete="CASCADE"), nullable=True, index=True
    )
    name: Mapped[str] = mapped_column(String, nullable=False)
    dataset_id: Mapped[str] = mapped_column(
        String, ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False
    )
    rules: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    color: Mapped[str | None] = mapped_column(String, nullable=True)
    base_config: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), onupdate=func.now(), nullable=True
    )

    model: Mapped["Model | None"] = relationship("Model", back_populates="scenarios")
    dataset: Mapped["Dataset"] = relationship("Dataset", back_populates="scenarios")


class SemanticColumn(Base):
    """Stores descriptions, synonyms, and agent-generated context for each column."""

    __tablename__ = "semantic_columns"
    __table_args__ = (
        UniqueConstraint("dataset_id", "column_name", name="uq_semantic_col"),
    )

    id: Mapped[str] = mapped_column(
        String, primary_key=True, server_default=func.gen_random_uuid().cast(String)
    )
    dataset_id: Mapped[str] = mapped_column(
        String, ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False
    )
    column_name: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    synonyms: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    value_source: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), onupdate=func.now(), nullable=True
    )

    dataset: Mapped["Dataset"] = relationship("Dataset", back_populates="semantic_columns")
    labels: Mapped[list["SemanticValueLabel"]] = relationship(
        "SemanticValueLabel", back_populates="semantic_column", cascade="all, delete-orphan"
    )


class SemanticValueLabel(Base):
    """Maps raw column values to human-readable labels (e.g. 400100 → 'Personnel Costs')."""

    __tablename__ = "semantic_value_labels"
    __table_args__ = (
        UniqueConstraint("semantic_column_id", "raw_value", name="uq_semantic_label"),
    )

    id: Mapped[str] = mapped_column(
        String, primary_key=True, server_default=func.gen_random_uuid().cast(String)
    )
    semantic_column_id: Mapped[str] = mapped_column(
        String, ForeignKey("semantic_columns.id", ondelete="CASCADE"), nullable=False
    )
    raw_value: Mapped[str] = mapped_column(String, nullable=False)
    display_label: Mapped[str] = mapped_column(String, nullable=False)
    category: Mapped[str | None] = mapped_column(String, nullable=True)
    sort_order: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    semantic_column: Mapped["SemanticColumn"] = relationship(
        "SemanticColumn", back_populates="labels"
    )


class TransformationStep(Base):
    """A replayable, auditable data transformation (reclassification, calculated column, etc.)."""

    __tablename__ = "transformation_steps"

    id: Mapped[str] = mapped_column(
        String, primary_key=True, server_default=func.gen_random_uuid().cast(String)
    )
    dataset_id: Mapped[str] = mapped_column(
        String, ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False
    )
    step_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    step_type: Mapped[str] = mapped_column(String, nullable=False)  # reclassification | calculated_column | rename | concat
    name: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    definition: Mapped[dict] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False, default="pending")  # pending | approved | applied | rejected
    created_by: Mapped[str] = mapped_column(String, nullable=False, default="user")  # user | ai_agent
    ai_prompt: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), onupdate=func.now(), nullable=True
    )

    dataset: Mapped["Dataset"] = relationship("Dataset", back_populates="transformations")


class KnowledgeEntry(Base):
    """Structured knowledge captured by the Data Understanding Agent about a dataset."""

    __tablename__ = "knowledge_entries"

    id: Mapped[str] = mapped_column(
        String, primary_key=True, server_default=func.gen_random_uuid().cast(String)
    )
    model_id: Mapped[str] = mapped_column(
        String, ForeignKey("models.id", ondelete="CASCADE"), nullable=False, index=True
    )
    dataset_id: Mapped[str] = mapped_column(
        String, ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False
    )
    # entry_type values (v2 — consolidated):
    #   "relationship"     — cross-table connection (with or without FK)
    #   "calculation"      — derived metric / formula (Level 2-ready)
    #   "transformation"   — data reshaping / aggregation rule (Level 2-ready)
    #   "definition"       — business term, field meaning, sign convention
    #   "note"             — data quirk, exception, free-form context
    entry_type: Mapped[str] = mapped_column(String, nullable=False)
    plain_text: Mapped[str] = mapped_column(Text, nullable=False)
    content: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    confidence: Mapped[str | None] = mapped_column(String, nullable=True)  # confirmed | suggested | rejected
    source: Mapped[str] = mapped_column(String, nullable=False, default="ai_agent")  # ai_agent | chat_agent | user | user_manual
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), onupdate=func.now(), nullable=True
    )

    model: Mapped["Model | None"] = relationship("Model", back_populates="knowledge_entries")
    dataset: Mapped["Dataset"] = relationship("Dataset", back_populates="knowledge_entries")
