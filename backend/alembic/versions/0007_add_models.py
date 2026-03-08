"""Add models table and model_id to existing tables.

Revision ID: 0007_add_models
Revises: 0006_consolidate_knowledge_types
Create Date: 2026-03-08
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0007_add_models"
down_revision = "0006_consolidate_knowledge_types"
branch_labels = None
depends_on = None

_DEFAULT_MODEL_ID = "00000000000000000000000000000001"
_DEFAULT_MODEL_NAME = "Default Model"


def _add_model_id(bind, inspector, table: str, fk_name: str) -> None:
    """Idempotently add model_id column, backfill, add FK and index."""
    existing_cols = {c["name"] for c in inspector.get_columns(table)}
    if "model_id" not in existing_cols:
        op.add_column(table, sa.Column("model_id", sa.String(), nullable=True))

    bind.execute(
        sa.text(f"UPDATE {table} SET model_id = :mid WHERE model_id IS NULL"),
        {"mid": _DEFAULT_MODEL_ID},
    )

    existing_fks = {fk["name"] for fk in inspector.get_foreign_keys(table)}
    if fk_name not in existing_fks:
        op.create_foreign_key(
            fk_name, table, "models", ["model_id"], ["id"], ondelete="CASCADE"
        )

    idx_name = f"ix_{table}_model_id"
    existing_indexes = {idx["name"] for idx in inspector.get_indexes(table)}
    if idx_name not in existing_indexes:
        op.create_index(idx_name, table, ["model_id"])


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    # 1. Create models table (idempotent)
    if not inspector.has_table("models"):
        op.create_table(
            "models",
            sa.Column("id", sa.String(), nullable=False),
            sa.Column("name", sa.String(), nullable=False),
            sa.Column("description", sa.Text(), nullable=True),
            sa.Column("status", sa.String(), nullable=False, server_default="active"),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                server_default=sa.text("now()"),
                nullable=False,
            ),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
            sa.PrimaryKeyConstraint("id"),
        )

    # 2. Insert default model (idempotent)
    bind.execute(
        sa.text(
            "INSERT INTO models (id, name, status) VALUES (:id, :name, 'active') "
            "ON CONFLICT (id) DO NOTHING"
        ),
        {"id": _DEFAULT_MODEL_ID, "name": _DEFAULT_MODEL_NAME},
    )

    # 3. datasets
    _add_model_id(bind, inspector, "datasets", "fk_datasets_model_id")

    # 4. scenarios
    if inspector.has_table("scenarios"):
        _add_model_id(bind, inspector, "scenarios", "fk_scenarios_model_id")

    # 5. knowledge_entries
    if inspector.has_table("knowledge_entries"):
        _add_model_id(bind, inspector, "knowledge_entries", "fk_knowledge_model_id")

    # 6. dataset_relationships (nullable — no backfill needed for relationships)
    if inspector.has_table("dataset_relationships"):
        existing_cols = {c["name"] for c in inspector.get_columns("dataset_relationships")}
        if "model_id" not in existing_cols:
            op.add_column(
                "dataset_relationships", sa.Column("model_id", sa.String(), nullable=True)
            )
        bind.execute(
            sa.text("UPDATE dataset_relationships SET model_id = :mid WHERE model_id IS NULL"),
            {"mid": _DEFAULT_MODEL_ID},
        )
        existing_fks = {fk["name"] for fk in inspector.get_foreign_keys("dataset_relationships")}
        if "fk_relationships_model_id" not in existing_fks:
            op.create_foreign_key(
                "fk_relationships_model_id",
                "dataset_relationships",
                "models",
                ["model_id"],
                ["id"],
                ondelete="CASCADE",
            )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if inspector.has_table("dataset_relationships"):
        try:
            op.drop_constraint("fk_relationships_model_id", "dataset_relationships", type_="foreignkey")
        except Exception:
            pass
        try:
            op.drop_column("dataset_relationships", "model_id")
        except Exception:
            pass

    if inspector.has_table("knowledge_entries"):
        try:
            op.drop_index("ix_knowledge_entries_model_id", table_name="knowledge_entries")
        except Exception:
            pass
        try:
            op.drop_constraint("fk_knowledge_model_id", "knowledge_entries", type_="foreignkey")
        except Exception:
            pass
        try:
            op.drop_column("knowledge_entries", "model_id")
        except Exception:
            pass

    if inspector.has_table("scenarios"):
        try:
            op.drop_index("ix_scenarios_model_id", table_name="scenarios")
        except Exception:
            pass
        try:
            op.drop_constraint("fk_scenarios_model_id", "scenarios", type_="foreignkey")
        except Exception:
            pass
        try:
            op.drop_column("scenarios", "model_id")
        except Exception:
            pass

    try:
        op.drop_index("ix_datasets_model_id", table_name="datasets")
    except Exception:
        pass
    try:
        op.drop_constraint("fk_datasets_model_id", "datasets", type_="foreignkey")
    except Exception:
        pass
    try:
        op.drop_column("datasets", "model_id")
    except Exception:
        pass

    op.drop_table("models")
