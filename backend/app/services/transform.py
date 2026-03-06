"""Transformation engine — converts JSON definitions to Polars expressions.

All transformations are deterministic: JSON definition → Polars expression.
No AI-generated code is ever executed — only structured declarative rules.

Supported step types:
  reclassification  — maps source column values to category labels
  calculated_column — arithmetic expression over existing columns
  rename            — rename a column
  concat            — concatenate multiple columns with a separator
"""

from __future__ import annotations

import logging
from typing import Any

import polars as pl
from sqlalchemy.engine import Engine
from sqlalchemy import text

logger = logging.getLogger(__name__)

# Storage helper (imported lazily to avoid circular imports)
def _storage():
    from app.services import storage as storage_svc
    return storage_svc

# Maps step_type → PostgreSQL data type string for ALTER TABLE ADD COLUMN
_PG_TYPE_MAP = {
    "text": "TEXT",
    "numeric": "NUMERIC(18,4)",
    "integer": "BIGINT",
    "boolean": "BOOLEAN",
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def apply_step(df: pl.DataFrame, step_definition: dict) -> pl.DataFrame:
    """Apply a single transformation step to a DataFrame. Returns a new DataFrame."""
    step_type = step_definition["step_type"]

    if step_type == "reclassification":
        return _apply_reclassification(df, step_definition)
    elif step_type == "calculated_column":
        return _apply_calculated(df, step_definition)
    elif step_type == "rename":
        return df.rename({step_definition["from_column"]: step_definition["to_column"]})
    elif step_type == "concat":
        return _apply_concat(df, step_definition)
    else:
        raise ValueError(f"Unknown step type: {step_type!r}")


def preview_step(df: pl.DataFrame, step_definition: dict, sample_size: int = 200) -> dict:
    """Preview a transformation without persisting anything.

    Returns a dict with:
        columns_before, columns_after, new_columns, removed_columns,
        new_column_stats (value counts for new categorical columns),
        affected_rows, sample_size
    """
    sample = df.head(sample_size)
    try:
        result = apply_step(sample, step_definition)
    except Exception as exc:
        return {"error": str(exc), "affected_rows": 0}

    new_cols = [c for c in result.columns if c not in sample.columns]
    removed_cols = [c for c in sample.columns if c not in result.columns]

    new_col_stats: dict[str, Any] = {}
    for col in new_cols:
        if col in result.columns:
            try:
                vc = result[col].drop_nulls().value_counts().sort("count", descending=True).head(30)
                new_col_stats[col] = {
                    str(row[col]): row["count"]
                    for row in vc.iter_rows(named=True)
                }
            except Exception:
                pass

    return {
        "columns_before": sample.columns,
        "columns_after": result.columns,
        "new_columns": new_cols,
        "removed_columns": removed_cols,
        "new_column_stats": new_col_stats,
        "affected_rows": len(result),
        "sample_size": sample_size,
    }


def materialize_step(engine: Engine, table_name: str, step_definition: dict) -> int:
    """Apply a transformation permanently to the actual PostgreSQL table.

    Returns the number of rows updated/affected.

    Strategy:
      - rename: ALTER TABLE RENAME COLUMN (no data read/write needed)
      - reclassification / calculated_column / concat:
          1. ALTER TABLE ADD COLUMN IF NOT EXISTS
          2. Read full table (only required source columns + _row_id)
          3. Apply step in Polars to get new column values
          4. Batch UPDATE back to PostgreSQL
    """
    svc = _storage()
    step_type = step_definition["step_type"]

    if step_type == "rename":
        from_col = step_definition["from_column"]
        to_col = step_definition["to_column"]
        with engine.begin() as conn:
            conn.execute(text(
                f'ALTER TABLE {_q(table_name)} RENAME COLUMN {_q(from_col)} TO {_q(to_col)}'
            ))
        logger.info("Renamed column %s → %s in %s", from_col, to_col, table_name)
        return 0

    # Determine output column and PG type
    output_col = step_definition.get("output_column") or step_definition.get("to_column", "")
    output_type = step_definition.get("output_type", "text")
    pg_type = _PG_TYPE_MAP.get(output_type, "TEXT")

    # 1. Add column (idempotent)
    with engine.begin() as conn:
        conn.execute(text(
            f'ALTER TABLE {_q(table_name)} ADD COLUMN IF NOT EXISTS {_q(output_col)} {pg_type}'
        ))

    # 2. Determine which source columns we need
    source_cols = _get_source_columns(step_definition)
    read_cols = list({"_row_id"} | source_cols)
    df = svc.read_dataset(engine, table_name, columns=read_cols)

    if df.is_empty():
        return 0

    # 3. Apply step
    result = apply_step(df, step_definition)

    if output_col not in result.columns:
        raise ValueError(f"Step did not produce expected output column '{output_col}'")

    # 4. Batch UPDATE
    row_count = _batch_update(engine, table_name, result["_row_id"].to_list(), result[output_col].to_list(), output_col)
    logger.info("Materialized %s column '%s' in %s (%d rows)", step_type, output_col, table_name, row_count)
    return row_count


# ---------------------------------------------------------------------------
# Step implementations
# ---------------------------------------------------------------------------

def _apply_reclassification(df: pl.DataFrame, defn: dict) -> pl.DataFrame:
    """Build a pl.when().then()...otherwise() chain from the rules list."""
    source_col = defn["source_column"]
    output_col = defn["output_column"]
    rules = defn["rules"]

    expr: pl.Expr | None = None
    default_val = "Other"

    for rule in rules:
        if "default" in rule:
            default_val = rule["default"] if rule["default"] is not None else "Other"
            continue

        cond = rule["condition"]
        result_val = rule["result"]
        op = cond["op"]

        if op == "between":
            lo, hi = str(cond["values"][0]), str(cond["values"][1])
            mask = pl.col(source_col).cast(pl.Utf8).is_between(lo, hi, closed="both")
        elif op == "in":
            vals = [str(v) for v in cond["values"]]
            mask = pl.col(source_col).cast(pl.Utf8).is_in(vals)
        elif op == "equals":
            mask = pl.col(source_col).cast(pl.Utf8) == str(cond["value"])
        elif op == "contains":
            mask = pl.col(source_col).cast(pl.Utf8).str.contains(str(cond["value"]), literal=True)
        elif op == "starts_with":
            mask = pl.col(source_col).cast(pl.Utf8).str.starts_with(str(cond["value"]))
        elif op == "not_equals":
            mask = pl.col(source_col).cast(pl.Utf8) != str(cond["value"])
        else:
            logger.warning("Unknown reclassification op: %s", op)
            continue

        if expr is None:
            expr = pl.when(mask).then(pl.lit(result_val))
        else:
            expr = expr.when(mask).then(pl.lit(result_val))

    if expr is None:
        return df.with_columns(pl.lit(default_val).alias(output_col))
    return df.with_columns(expr.otherwise(pl.lit(default_val)).alias(output_col))


def _apply_calculated(df: pl.DataFrame, defn: dict) -> pl.DataFrame:
    """Build arithmetic Polars expression from the nested op tree."""
    output_col = defn["output_column"]
    expr_def = defn["expression"]
    null_handling = expr_def.get("null_handling", "null")
    polars_expr = _build_arithmetic_expr(expr_def, null_handling)
    return df.with_columns(polars_expr.alias(output_col))


def _build_arithmetic_expr(expr_def: dict, null_handling: str) -> pl.Expr:
    """Recursively build a Polars expression from the JSON definition."""
    if "column" in expr_def:
        e = pl.col(expr_def["column"]).cast(pl.Float64, strict=False)
        if null_handling == "zero":
            e = e.fill_null(0.0)
        return e

    if "literal" in expr_def:
        return pl.lit(float(expr_def["literal"]))

    op = expr_def["op"]

    # Unary ops
    if op in ("abs", "negate", "round"):
        left = _build_arithmetic_expr(expr_def["left"], null_handling)
        if op == "abs":
            return left.abs()
        if op == "negate":
            return left * -1
        if op == "round":
            return left.round(expr_def.get("decimals", 2))

    # Binary ops
    left = _build_arithmetic_expr(expr_def["left"], null_handling)
    right = _build_arithmetic_expr(expr_def["right"], null_handling)

    if op == "add":
        return left + right
    if op == "subtract":
        return left - right
    if op == "multiply":
        return left * right
    if op == "divide":
        # Avoid division by zero
        return pl.when(right != 0).then(left / right).otherwise(pl.lit(None))

    raise ValueError(f"Unknown arithmetic op: {op!r}")


def _apply_concat(df: pl.DataFrame, defn: dict) -> pl.DataFrame:
    """Concatenate multiple columns with a separator."""
    cols = defn["columns"]
    sep = defn.get("separator", " ")
    output_col = defn["output_column"]

    if not cols:
        return df.with_columns(pl.lit("").alias(output_col))

    expr = pl.col(cols[0]).cast(pl.Utf8).fill_null("")
    for col in cols[1:]:
        expr = expr + pl.lit(sep) + pl.col(col).cast(pl.Utf8).fill_null("")

    return df.with_columns(expr.alias(output_col))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_source_columns(step_definition: dict) -> set[str]:
    """Extract all source column names referenced in a step definition."""
    step_type = step_definition["step_type"]
    cols: set[str] = set()

    if step_type == "reclassification":
        cols.add(step_definition["source_column"])
    elif step_type == "calculated_column":
        _collect_expr_columns(step_definition["expression"], cols)
    elif step_type == "concat":
        cols.update(step_definition["columns"])
    elif step_type == "rename":
        cols.add(step_definition["from_column"])

    return cols


def _collect_expr_columns(expr_def: dict, cols: set[str]) -> None:
    """Recursively collect column names from an arithmetic expression tree."""
    if "column" in expr_def:
        cols.add(expr_def["column"])
    if "left" in expr_def:
        _collect_expr_columns(expr_def["left"], cols)
    if "right" in expr_def:
        _collect_expr_columns(expr_def["right"], cols)


def _q(name: str) -> str:
    """Double-quote a PostgreSQL identifier."""
    return '"' + name.replace('"', '""') + '"'


def _batch_update(
    engine: Engine,
    table_name: str,
    row_ids: list,
    values: list,
    col_name: str,
    batch_size: int = 5000,
) -> int:
    """Batch UPDATE a single column using VALUES CTE for much better performance."""
    if not row_ids:
        return 0

    tbl = _q(table_name)
    col = _q(col_name)
    total = 0
    with engine.begin() as conn:
        for start in range(0, len(row_ids), batch_size):
            batch_ids = row_ids[start: start + batch_size]
            batch_vals = values[start: start + batch_size]
            # Build parameterised VALUES list: (:rid_0::bigint, :val_0), ...
            value_rows = []
            params: dict = {}
            for i, (rid, val) in enumerate(zip(batch_ids, batch_vals)):
                params[f"rid_{i}"] = rid
                params[f"val_{i}"] = _safe_val(val)
                value_rows.append(f"(:rid_{i}::bigint, :val_{i})")
            values_sql = ", ".join(value_rows)
            stmt = text(
                f"UPDATE {tbl} AS t "
                f"SET {col} = v.new_val "
                f"FROM (VALUES {values_sql}) AS v(row_id, new_val) "
                f'WHERE t."_row_id" = v.row_id'
            )
            conn.execute(stmt, params)
            total += len(batch_ids)
    return total


def _safe_val(v: Any) -> Any:
    """Coerce Polars null/NaN to Python None for SQL binding."""
    import math
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    return v
