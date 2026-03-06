"""Add semantic layer: semantic_columns, semantic_value_labels, agent_context_notes.

Revision ID: 0002_semantic_layer
Revises: 0001_add_ai_fields
Create Date: 2026-03-06
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0002_semantic_layer"
down_revision = "0001_add_ai_fields"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # datasets: structured AI context notes for the scenario agent
    op.add_column(
        "datasets",
        sa.Column("agent_context_notes", sa.JSON(), nullable=True),
    )

    # semantic_columns: per-column descriptions, synonyms, value_source
    op.create_table(
        "semantic_columns",
        sa.Column("id", sa.String(), nullable=False, primary_key=True),
        sa.Column("dataset_id", sa.String(), sa.ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False),
        sa.Column("column_name", sa.String(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("synonyms", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("value_source", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("dataset_id", "column_name", name="uq_semantic_col"),
    )
    op.create_index("ix_semantic_columns_dataset_id", "semantic_columns", ["dataset_id"])

    # semantic_value_labels: raw_value → display_label mappings
    op.create_table(
        "semantic_value_labels",
        sa.Column("id", sa.String(), nullable=False, primary_key=True),
        sa.Column("semantic_column_id", sa.String(), sa.ForeignKey("semantic_columns.id", ondelete="CASCADE"), nullable=False),
        sa.Column("raw_value", sa.String(), nullable=False),
        sa.Column("display_label", sa.String(), nullable=False),
        sa.Column("category", sa.String(), nullable=True),
        sa.Column("sort_order", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.UniqueConstraint("semantic_column_id", "raw_value", name="uq_semantic_label"),
    )
    op.create_index("ix_semantic_value_labels_col_id", "semantic_value_labels", ["semantic_column_id"])


def downgrade() -> None:
    op.drop_index("ix_semantic_value_labels_col_id", table_name="semantic_value_labels")
    op.drop_table("semantic_value_labels")
    op.drop_index("ix_semantic_columns_dataset_id", table_name="semantic_columns")
    op.drop_table("semantic_columns")
    op.drop_column("datasets", "agent_context_notes")
