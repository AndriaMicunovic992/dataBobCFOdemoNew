"""Add transformation_steps table.

Revision ID: 0003_transformation_steps
Revises: 0002_semantic_layer
Create Date: 2026-03-06
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0003_transformation_steps"
down_revision = "0002_semantic_layer"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "transformation_steps",
        sa.Column("id", sa.String(), nullable=False, primary_key=True),
        sa.Column("dataset_id", sa.String(), sa.ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False),
        sa.Column("step_order", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("step_type", sa.String(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("definition", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(), nullable=False, server_default="pending"),
        sa.Column("created_by", sa.String(), nullable=False, server_default="user"),
        sa.Column("ai_prompt", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_transformation_steps_dataset_id", "transformation_steps", ["dataset_id"])
    op.create_index("ix_transformation_steps_status", "transformation_steps", ["dataset_id", "status"])


def downgrade() -> None:
    op.drop_index("ix_transformation_steps_status", table_name="transformation_steps")
    op.drop_index("ix_transformation_steps_dataset_id", table_name="transformation_steps")
    op.drop_table("transformation_steps")
