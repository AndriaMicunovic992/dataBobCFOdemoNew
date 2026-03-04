"""SQLAlchemy models for dataset metadata catalog."""

import uuid
from datetime import datetime

from sqlalchemy import (
    String, DateTime, Integer, Text, ForeignKey, JSON, UniqueConstraint
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from app.database import Base


class Dataset(Base):
    """Tracks a parsed dataset and its dynamic PostgreSQL table."""

    __tablename__ = "datasets"

    id: Mapped[str] = mapped_column(
        String, primary_key=True, server_default=func.gen_random_uuid().cast(String)
    )
    name: Mapped[str] = mapped_column(String, nullable=False)
    table_name: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    source_filename: Mapped[str | None] = mapped_column(String, nullable=True)
    row_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    status: Mapped[str] = mapped_column(String, nullable=False, default="processing")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), onupdate=func.now(), nullable=True
    )

    columns: Mapped[list["DatasetColumn"]] = relationship(
        "DatasetColumn", back_populates="dataset", cascade="all, delete-orphan"
    )
    scenarios: Mapped[list["Scenario"]] = relationship(
        "Scenario", back_populates="dataset", cascade="all, delete-orphan"
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

    dataset: Mapped["Dataset"] = relationship("Dataset", back_populates="columns")


class DatasetRelationship(Base):
    """Detected or user-defined join relationship between two datasets."""

    __tablename__ = "dataset_relationships"

    id: Mapped[str] = mapped_column(
        String, primary_key=True, server_default=func.gen_random_uuid().cast(String)
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
    name: Mapped[str] = mapped_column(String, nullable=False)
    dataset_id: Mapped[str] = mapped_column(
        String, ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False
    )
    rules: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    color: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), onupdate=func.now(), nullable=True
    )

    dataset: Mapped["Dataset"] = relationship("Dataset", back_populates="scenarios")
