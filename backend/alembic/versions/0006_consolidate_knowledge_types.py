"""Consolidate knowledge entry types to 5 canonical types.

Revision ID: 0006_consolidate_knowledge_types
Revises: 0005_knowledge_entries
Create Date: 2026-03-08
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0006_consolidate_knowledge_types"
down_revision = "0005_knowledge_entries"
branch_labels = None
depends_on = None

# Maps old verbose types → consolidated v2 types
TYPE_MAP = {
    "term_definition": "definition",
    "interpretation_rule": "definition",
    "metric_definition": "calculation",
    "annotation": "note",
    "dataset_mapping": "relationship",
    "relationship_hint": "relationship",
    "unmapped_relationship": "relationship",
    "data_transformation": "transformation",
}


def upgrade() -> None:
    conn = op.get_bind()
    for old_type, new_type in TYPE_MAP.items():
        conn.execute(
            sa.text(
                "UPDATE knowledge_entries SET entry_type = :new WHERE entry_type = :old"
            ),
            {"old": old_type, "new": new_type},
        )


def downgrade() -> None:
    # Best-effort reverse (many-to-one cannot be fully reversed)
    reverse = {
        "definition": "term_definition",
        "calculation": "metric_definition",
        "note": "annotation",
        "relationship": "dataset_mapping",
        "transformation": "data_transformation",
    }
    conn = op.get_bind()
    for new_type, old_type in reverse.items():
        conn.execute(
            sa.text(
                "UPDATE knowledge_entries SET entry_type = :old WHERE entry_type = :new"
            ),
            {"old": old_type, "new": new_type},
        )
