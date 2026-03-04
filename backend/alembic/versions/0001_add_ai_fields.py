"""Add AI schema-agent fields to datasets and dataset_columns.

Revision ID: 0001_add_ai_fields
Revises:
Create Date: 2026-03-04
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0001_add_ai_fields"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # datasets: ai_analyzed flag + ai_notes JSON blob
    op.add_column(
        "datasets",
        sa.Column("ai_analyzed", sa.Boolean(), nullable=False, server_default="false"),
    )
    op.add_column(
        "datasets",
        sa.Column("ai_notes", sa.JSON(), nullable=True),
    )

    # dataset_columns: per-column AI reasoning
    op.add_column(
        "dataset_columns",
        sa.Column("ai_suggestion", sa.JSON(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("dataset_columns", "ai_suggestion")
    op.drop_column("datasets", "ai_notes")
    op.drop_column("datasets", "ai_analyzed")
