"""Add settings JSON column to models.

Revision ID: 0010_model_settings
Revises: 0009_knowledge_model_required
Create Date: 2026-03-10
"""
from __future__ import annotations
from alembic import op
import sqlalchemy as sa

revision = "0010_model_settings"
down_revision = "0009_knowledge_model_required"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "models",
        sa.Column("settings", sa.JSON(), nullable=True, server_default="{}"),
    )


def downgrade() -> None:
    op.drop_column("models", "settings")
