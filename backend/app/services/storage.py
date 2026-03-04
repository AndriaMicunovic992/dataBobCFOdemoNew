"""Dynamic table creation and data loading service.

All DDL and bulk-load operations use the synchronous engine because asyncpg
does not support server-side COPY or DDL with raw connections reliably.
"""

from __future__ import annotations

import io
import logging
import os
import secrets
from typing import Any

import polars as pl
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    MetaData,
    Numeric,
    Table,
    Text,
    inspect,
    text,
)
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# Metadata instance used only for dynamic table reflection / creation
_dynamic_meta = MetaData()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def generate_table_name() -> str:
    """Return a unique safe table name, e.g. 'ds_a1b2c3d4'."""
    return "ds_" + secrets.token_hex(4)


def _pg_type(data_type: str) -> Any:
    """Map a detected data_type string to a SQLAlchemy column type."""
    mapping = {
        "text": Text,
        "numeric": lambda: Numeric(18, 4),
        "integer": BigInteger,
        "date": Date,
        "boolean": Boolean,
    }
    factory = mapping.get(data_type, Text)
    return factory() if callable(factory) else factory


def _quoted(name: str) -> str:
    """Double-quote an identifier for use in raw SQL."""
    return '"' + name.replace('"', '""') + '"'


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

def create_dataset_table(engine: Engine, table_name: str, columns: list[dict]) -> None:
    """
    Create a typed PostgreSQL table for a dataset.

    ``columns`` is a list of dicts with at least:
        column_name, data_type, column_role, unique_count (optional)

    A ``_row_id`` BigInteger primary key is added automatically.
    Indexes are created on key / time columns and low-cardinality attribute columns.
    """
    sa_columns = [Column("_row_id", BigInteger, primary_key=True, autoincrement=True)]
    for col in columns:
        sa_columns.append(
            Column(col["column_name"], _pg_type(col["data_type"]), nullable=True)
        )

    table = Table(table_name, _dynamic_meta, *sa_columns)

    with engine.begin() as conn:
        _dynamic_meta.create_all(conn, tables=[table], checkfirst=True)
        logger.info("Created table %s (%d user columns)", table_name, len(columns))

        # Indexes
        for col in columns:
            role = col.get("column_role", "attribute")
            unique_count = col.get("unique_count") or 0
            should_index = role in ("key", "time") or (
                role == "attribute" and 0 < unique_count < 1000
            )
            if should_index:
                idx_name = f"idx_{table_name}_{col['column_name']}"[:63]
                col_quoted = _quoted(col["column_name"])
                tbl_quoted = _quoted(table_name)
                conn.execute(
                    text(
                        f"CREATE INDEX IF NOT EXISTS {_quoted(idx_name)} "
                        f"ON {tbl_quoted} ({col_quoted})"
                    )
                )
                logger.debug("Created index %s", idx_name)

        conn.execute(text(f"ANALYZE {_quoted(table_name)}"))
        logger.debug("ANALYZE %s done", table_name)


def drop_dataset_table(engine: Engine, table_name: str) -> None:
    """DROP TABLE IF EXISTS <table_name> CASCADE."""
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {_quoted(table_name)} CASCADE"))
    logger.info("Dropped table %s", table_name)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _coerce_dataframe(df: pl.DataFrame, columns: list[dict]) -> pl.DataFrame:
    """
    Cast Polars columns to types that map cleanly to the PG target types.
    Unknown columns (e.g. _row_id) are skipped.
    """
    col_map = {c["column_name"]: c["data_type"] for c in columns}
    casts = []
    for col_name in df.columns:
        dtype = col_map.get(col_name)
        if dtype == "integer":
            casts.append(pl.col(col_name).cast(pl.Int64, strict=False).alias(col_name))
        elif dtype == "numeric":
            casts.append(pl.col(col_name).cast(pl.Float64, strict=False).alias(col_name))
        elif dtype == "boolean":
            casts.append(pl.col(col_name).cast(pl.Boolean, strict=False).alias(col_name))
        elif dtype == "date":
            # Try casting; leave as string if it fails (PG will coerce from text)
            if df[col_name].dtype not in (pl.Date, pl.Datetime):
                try:
                    casts.append(
                        pl.col(col_name)
                        .str.strptime(pl.Date, format=None, strict=False)
                        .alias(col_name)
                    )
                except Exception:
                    pass
        else:
            casts.append(pl.col(col_name).cast(pl.Utf8, strict=False).alias(col_name))
    if casts:
        df = df.with_columns(casts)
    return df


def _copy_from_csv(conn_raw: Any, table_name: str, df: pl.DataFrame) -> None:
    """Bulk-load via PostgreSQL COPY FROM STDIN (CSV). Fastest path."""
    buf = io.BytesIO()
    df.write_csv(buf, null_value="")
    buf.seek(0)

    # psycopg2 raw connection
    with conn_raw.cursor() as cur:
        quoted_cols = ", ".join(_quoted(c) for c in df.columns)
        cur.copy_expert(
            f"COPY {_quoted(table_name)} ({quoted_cols}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '')",
            buf,
        )
    logger.debug("COPY loaded %d rows into %s", len(df), table_name)


def _insert_batches(conn: Any, table_name: str, df: pl.DataFrame, batch_size: int = 5000) -> None:
    """Fallback: chunked INSERT via SQLAlchemy Core."""
    quoted_tbl = _quoted(table_name)
    quoted_cols = ", ".join(_quoted(c) for c in df.columns)
    placeholders = ", ".join(f":{c}" for c in df.columns)
    stmt = text(f"INSERT INTO {quoted_tbl} ({quoted_cols}) VALUES ({placeholders})")

    rows = df.to_dicts()
    for start in range(0, len(rows), batch_size):
        batch = rows[start : start + batch_size]
        conn.execute(stmt, batch)
        logger.debug("Inserted rows %d–%d into %s", start, start + len(batch), table_name)


def load_data(
    engine: Engine,
    table_name: str,
    df: pl.DataFrame,
    columns: list[dict],
) -> int:
    """
    Bulk-load a Polars DataFrame into the dynamic table.

    Tries PostgreSQL COPY first (fastest); falls back to chunked INSERT.
    Returns the number of rows loaded.
    """
    if df.is_empty():
        logger.warning("load_data called with empty DataFrame for %s", table_name)
        return 0

    df = _coerce_dataframe(df, columns)
    row_count = len(df)

    with engine.begin() as conn:
        # Attempt COPY via raw psycopg2 connection
        try:
            raw = conn.connection  # underlying DBAPI connection
            _copy_from_csv(raw, table_name, df)
            logger.info("COPY loaded %d rows into %s", row_count, table_name)
        except Exception as exc:
            logger.warning("COPY failed (%s), falling back to batched INSERT", exc)
            _insert_batches(conn, table_name, df)
            logger.info("INSERT loaded %d rows into %s", row_count, table_name)

    return row_count


# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------

def read_dataset(
    engine: Engine,
    table_name: str,
    columns: list[str] | None = None,
    filters: dict[str, Any] | None = None,
    limit: int | None = None,
) -> pl.DataFrame:
    """
    Read rows from a dynamic table into a Polars DataFrame.

    ``filters`` is a simple equality map: {column_name: value}.
    For range / complex filters, build the SQL upstream and pass via text().
    """
    if columns:
        col_clause = ", ".join(_quoted(c) for c in columns)
    else:
        col_clause = "*"

    sql = f"SELECT {col_clause} FROM {_quoted(table_name)}"
    params: dict[str, Any] = {}

    if filters:
        clauses = []
        for i, (col, val) in enumerate(filters.items()):
            param_key = f"f{i}"
            clauses.append(f"{_quoted(col)} = :{param_key}")
            params[param_key] = val
        sql += " WHERE " + " AND ".join(clauses)

    if limit is not None:
        sql += f" LIMIT {int(limit)}"

    logger.debug("read_dataset SQL: %s | params=%s", sql, params)

    with engine.connect() as conn:
        result = conn.execute(text(sql), params)
        col_names = list(result.keys())
        rows = result.fetchall()

    if not rows:
        return pl.DataFrame({c: [] for c in col_names})

    # Build column-oriented dict for Polars
    data: dict[str, list] = {c: [] for c in col_names}
    for row in rows:
        for c, v in zip(col_names, row):
            data[c].append(v)

    return pl.DataFrame(data, infer_schema_length=500)
