"""Add knowledge_entries table.

Revision ID: 0005_knowledge_entries
Revises: 0004_scenario_base_config
Create Date: 2026-03-08
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0005_knowledge_entries"
down_revision = "0004_scenario_base_config"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "knowledge_entries",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("dataset_id", sa.String(), nullable=False),
        sa.Column("entry_type", sa.String(), nullable=False),
        sa.Column("plain_text", sa.Text(), nullable=False),
        sa.Column("content", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("confidence", sa.String(), nullable=True),
        sa.Column("source", sa.String(), nullable=False, server_default="ai_agent"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["dataset_id"], ["datasets.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_knowledge_entries_dataset_id", "knowledge_entries", ["dataset_id"])
    op.create_index("ix_knowledge_entries_entry_type", "knowledge_entries", ["entry_type"])


def downgrade() -> None:
    op.drop_index("ix_knowledge_entries_entry_type", table_name="knowledge_entries")
    op.drop_index("ix_knowledge_entries_dataset_id", table_name="knowledge_entries")
    op.drop_table("knowledge_entries")
