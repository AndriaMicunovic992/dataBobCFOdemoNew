"""Initial schema: datasets, dataset_columns, dataset_relationships, scenarios.

Revision ID: 78181f2bca1c
Revises:
Create Date: 2026-03-04
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "78181f2bca1c"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "datasets",
        sa.Column("id", sa.String(), server_default=sa.text("gen_random_uuid()::text"), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("table_name", sa.String(), nullable=False),
        sa.Column("source_filename", sa.String(), nullable=True),
        sa.Column("row_count", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("table_name"),
    )

    op.create_table(
        "dataset_columns",
        sa.Column("id", sa.String(), server_default=sa.text("gen_random_uuid()::text"), nullable=False),
        sa.Column("dataset_id", sa.String(), nullable=False),
        sa.Column("column_name", sa.String(), nullable=False),
        sa.Column("display_name", sa.String(), nullable=False),
        sa.Column("data_type", sa.String(), nullable=False),
        sa.Column("column_role", sa.String(), nullable=False),
        sa.Column("unique_count", sa.Integer(), nullable=True),
        sa.Column("sample_values", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["dataset_id"], ["datasets.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("dataset_id", "column_name", name="uq_dataset_column"),
    )

    op.create_table(
        "dataset_relationships",
        sa.Column("id", sa.String(), server_default=sa.text("gen_random_uuid()::text"), nullable=False),
        sa.Column("source_dataset_id", sa.String(), nullable=False),
        sa.Column("target_dataset_id", sa.String(), nullable=False),
        sa.Column("source_column", sa.String(), nullable=False),
        sa.Column("target_column", sa.String(), nullable=False),
        sa.Column("coverage_pct", sa.Integer(), nullable=True),
        sa.Column("overlap_count", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.ForeignKeyConstraint(["source_dataset_id"], ["datasets.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["target_dataset_id"], ["datasets.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "scenarios",
        sa.Column("id", sa.String(), server_default=sa.text("gen_random_uuid()::text"), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("dataset_id", sa.String(), nullable=False),
        sa.Column("rules", sa.JSON(), nullable=False),
        sa.Column("color", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["dataset_id"], ["datasets.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    op.drop_table("scenarios")
    op.drop_table("dataset_relationships")
    op.drop_table("dataset_columns")
    op.drop_table("datasets")
