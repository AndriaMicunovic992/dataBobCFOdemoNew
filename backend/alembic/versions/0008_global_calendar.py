"""Make _calendar dataset global (model_id = NULL) so it is visible in all models.

Revision ID: 0008_global_calendar
Revises: 0007_add_models
Create Date: 2026-03-09
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0008_global_calendar"
down_revision = "0007_add_models"
branch_labels = None
depends_on = None

_DEFAULT_MODEL_ID = "00000000000000000000000000000001"


def upgrade() -> None:
    conn = op.get_bind()

    # Make the _calendar dataset global (no model ownership)
    conn.execute(sa.text("""
        UPDATE datasets
        SET model_id = NULL
        WHERE name = '_calendar'
           OR table_name = 'dim_calendar'
    """))

    # Make relationships that link TO the calendar dataset global as well
    # (auto_link_calendar already creates them without model_id, but fix any
    #  that were created with model_id set)
    conn.execute(sa.text("""
        UPDATE dataset_relationships
        SET model_id = NULL
        WHERE target_dataset_id IN (
            SELECT id FROM datasets
            WHERE name = '_calendar' OR table_name = 'dim_calendar'
        )
    """))


def downgrade() -> None:
    conn = op.get_bind()

    # Move calendar back to the default model
    conn.execute(sa.text(f"""
        UPDATE datasets
        SET model_id = '{_DEFAULT_MODEL_ID}'
        WHERE model_id IS NULL
          AND (name = '_calendar' OR table_name = 'dim_calendar')
    """))

    conn.execute(sa.text(f"""
        UPDATE dataset_relationships
        SET model_id = '{_DEFAULT_MODEL_ID}'
        WHERE model_id IS NULL
          AND target_dataset_id IN (
              SELECT id FROM datasets
              WHERE name = '_calendar' OR table_name = 'dim_calendar'
          )
    """))
