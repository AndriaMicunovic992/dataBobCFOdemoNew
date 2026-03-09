"""Make knowledge_entries.model_id NOT NULL — entries are strictly per model.

Revision ID: 0009_knowledge_model_required
Revises: 0008_global_calendar
Create Date: 2026-03-09
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0009_knowledge_model_required"
down_revision = "0008_global_calendar"
branch_labels = None
depends_on = None

_DEFAULT_MODEL_ID = "00000000000000000000000000000001"


def upgrade() -> None:
    conn = op.get_bind()

    # Assign any orphan entries (NULL model_id) to the default model
    conn.execute(sa.text(f"""
        UPDATE knowledge_entries
        SET model_id = '{_DEFAULT_MODEL_ID}'
        WHERE model_id IS NULL
    """))

    # Now make the column NOT NULL
    op.alter_column("knowledge_entries", "model_id", nullable=False)


def downgrade() -> None:
    op.alter_column("knowledge_entries", "model_id", nullable=True)
