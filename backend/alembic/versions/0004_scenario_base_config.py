"""Add base_config JSON column to scenarios.

Revision ID: 0004_scenario_base_config
Revises: 0003_transformation_steps
Create Date: 2026-03-06
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0004_scenario_base_config"
down_revision = "0003_transformation_steps"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "scenarios",
        sa.Column("base_config", sa.JSON(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("scenarios", "base_config")
