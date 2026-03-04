"""Scenario computation engine using Polars.

Replaces the client-side applyRules() function from the React prototype.
Performance target: 100K rows × 5 rules < 100 ms.

Rule shape (stored as JSON in Scenario.rules):
{
    "id": "string",
    "name": "string",
    "type": "multiplier" | "offset",
    "factor": 1.05,          // used when type == "multiplier"
    "offset": 100000,        // used when type == "offset" (total amount added)
    "filters": {             // optional; all keys AND-ed together
        "category": ["Personalaufwand", "Warenaufwand"],
        "company_id": [8]
    },
    "periodFrom": "2025-01", // optional ISO string
    "periodTo": "2025-12"    // optional ISO string
}
"""

from __future__ import annotations

import logging
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)

# Column names that are treated as period/time for range filtering.
_PERIOD_COLUMN_CANDIDATES = (
    "period", "periode", "monat", "month", "date", "datum", "buchungsdatum",
    "fiscal_period", "year_month",
)


def _find_period_col(df: pl.DataFrame) -> str | None:
    """Return the first period-like column found in the DataFrame, or None."""
    lowered = {c.lower(): c for c in df.columns}
    for candidate in _PERIOD_COLUMN_CANDIDATES:
        if candidate in lowered:
            return lowered[candidate]
    return None


def _build_mask(df: pl.LazyFrame, rule: dict) -> pl.Expr:
    """
    Build a boolean Polars expression from a rule's filters + period range.

    Returns an expression that evaluates to True for rows the rule applies to.
    If no filters / period constraints are given, every row matches.
    """
    exprs: list[pl.Expr] = []

    # --- Categorical / value filters ---
    for col_name, values in (rule.get("filters") or {}).items():
        if col_name not in df.collect_schema().names():
            logger.warning("Filter column %r not found in DataFrame, skipping", col_name)
            continue
        str_vals = [str(v) for v in values]
        exprs.append(pl.col(col_name).cast(pl.Utf8).is_in(str_vals))

    # --- Period range ---
    period_from: str | None = rule.get("periodFrom")
    period_to: str | None = rule.get("periodTo")
    if period_from or period_to:
        schema = df.collect_schema()
        # Find a period column in the schema (lazy frame knows column names)
        period_col: str | None = None
        for candidate in _PERIOD_COLUMN_CANDIDATES:
            for c in schema.names():
                if c.lower() == candidate:
                    period_col = c
                    break
            if period_col:
                break

        if period_col:
            period_expr = pl.col(period_col).cast(pl.Utf8)
            if period_from:
                exprs.append(period_expr >= period_from)
            if period_to:
                exprs.append(period_expr <= period_to)
        else:
            logger.warning("periodFrom/periodTo specified but no period column found")

    if not exprs:
        return pl.lit(True)

    mask = exprs[0]
    for e in exprs[1:]:
        mask = mask & e
    return mask


def _apply_multiplier(
    lf: pl.LazyFrame, mask: pl.Expr, value_column: str, factor: float
) -> pl.LazyFrame:
    """Multiply value_column by factor where mask is True."""
    return lf.with_columns(
        pl.when(mask)
        .then(pl.col(value_column).cast(pl.Float64) * factor)
        .otherwise(pl.col(value_column).cast(pl.Float64))
        .alias(value_column)
    )


def _apply_offset(
    lf: pl.LazyFrame, mask: pl.Expr, value_column: str, total_offset: float
) -> pl.LazyFrame:
    """
    Distribute total_offset evenly across matched rows, period-aware.

    Algorithm:
      1. Stamp every row with a stable __row_idx.
      2. Filter to matching rows (preserving their original indices).
      3. Count matching rows per period → per_period_rows.
      4. Count distinct periods in matching rows → num_periods.
      5. per_period_amount = total_offset / num_periods.
      6. per_row_delta = per_period_amount / per_period_rows.
      7. Left-join deltas back onto full frame by __row_idx; fill 0 elsewhere.

    This ensures: 100 K offset across 12 months → each month gets ~8 333,
    regardless of how many rows exist per month.
    """
    # Stamp stable row indices BEFORE filtering so they survive the round-trip.
    df_full = lf.collect().with_row_index("__row_idx")

    # Filter uses mask as a plain Expr — works on eager DataFrames too.
    df_match = df_full.filter(mask)

    if df_match.is_empty():
        # Nothing matched; drop the temporary index column and return.
        return df_full.drop("__row_idx").lazy()

    period_col = _find_period_col(df_match)

    if period_col:
        num_periods = df_match[period_col].cast(pl.Utf8).n_unique()
        per_period = total_offset / max(num_periods, 1)

        # Compute per-row delta = per_period / rows_in_that_period
        period_counts = (
            df_match.group_by(period_col)
            .agg(pl.len().alias("_period_row_count"))
        )
        df_match = df_match.join(period_counts, on=period_col, how="left")
        df_match = df_match.with_columns(
            (pl.lit(per_period) / pl.col("_period_row_count")).alias("_per_row_delta")
        ).drop("_period_row_count")
    else:
        # No period column: distribute evenly across all matching rows.
        n_rows = len(df_match)
        df_match = df_match.with_columns(
            pl.lit(total_offset / max(n_rows, 1)).alias("_per_row_delta")
        )

    # Left-join deltas back by the stable row index.
    delta_df = df_match.select(["__row_idx", "_per_row_delta"])
    df_merged = df_full.join(delta_df, on="__row_idx", how="left")
    df_merged = df_merged.with_columns(
        (
            pl.col(value_column).cast(pl.Float64)
            + pl.col("_per_row_delta").fill_null(0.0)
        ).alias(value_column)
    ).drop(["__row_idx", "_per_row_delta"])

    return df_merged.lazy()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def apply_rules(
    df: pl.DataFrame,
    rules: list[dict],
    value_column: str = "amount",
) -> pl.DataFrame:
    """Apply scenario rules sequentially to a DataFrame.

    Preserves original values in a ``baseline_<value_column>`` column.
    Rules are applied in order — later rules see the already-modified values.

    Args:
        df: Input DataFrame (typically the enriched baseline).
        rules: List of rule dicts (see module docstring for shape).
        value_column: Name of the numeric column to modify.

    Returns:
        Modified DataFrame with an additional ``baseline_<value_column>`` column.
    """
    if value_column not in df.columns:
        logger.warning("value_column %r not found in DataFrame", value_column)
        return df

    # Preserve originals before any modification
    baseline_col = f"baseline_{value_column}"
    if baseline_col not in df.columns:
        df = df.with_columns(
            pl.col(value_column).cast(pl.Float64).alias(baseline_col)
        )

    lf = df.lazy()

    for rule in rules:
        rule_type = rule.get("type", "multiplier")
        mask = _build_mask(lf, rule)

        if rule_type == "multiplier":
            factor = float(rule.get("factor", 1.0))
            lf = _apply_multiplier(lf, mask, value_column, factor)

        elif rule_type == "offset":
            total_offset = float(rule.get("offset", 0.0))
            if total_offset != 0.0:
                lf = _apply_offset(lf, mask, value_column, total_offset)

        else:
            logger.warning("Unknown rule type %r, skipping", rule_type)

    return lf.collect()


def compute_variance(
    baseline: pl.DataFrame,
    scenario: pl.DataFrame,
    group_by: list[str],
    value_column: str = "amount",
) -> dict[str, Any]:
    """Compare baseline vs scenario by group.

    Both DataFrames are grouped by ``group_by``, the ``value_column`` is
    summed, and delta / delta_pct are computed per group.

    Returns::

        {
            "groups": [
                {
                    "group": {"period": "2025-01", ...},
                    "actual": 100000.0,
                    "scenario": 105000.0,
                    "delta": 5000.0,
                    "delta_pct": 5.0
                },
                ...
            ],
            "total_actual": 1_200_000.0,
            "total_scenario": 1_260_000.0,
            "total_delta": 60_000.0,
            "total_delta_pct": 5.0
        }
    """
    valid_gb = [c for c in group_by if c in baseline.columns and c in scenario.columns]

    def _agg(df: pl.DataFrame) -> pl.DataFrame:
        return (
            df.lazy()
            .group_by(valid_gb)
            .agg(pl.col(value_column).cast(pl.Float64).sum().alias("_val"))
            .collect()
        )

    base_agg = _agg(baseline)
    scen_agg = _agg(scenario)

    joined = base_agg.join(scen_agg, on=valid_gb, how="outer_coalesce", suffix="_scen")
    joined = joined.rename({"_val": "actual", "_val_scen": "scenario"})
    joined = joined.with_columns(
        pl.col("actual").fill_null(0.0),
        pl.col("scenario").fill_null(0.0),
    ).with_columns(
        (pl.col("scenario") - pl.col("actual")).alias("delta")
    ).with_columns(
        pl.when(pl.col("actual") != 0)
        .then((pl.col("delta") / pl.col("actual").abs()) * 100)
        .otherwise(pl.lit(None))
        .alias("delta_pct")
    )

    groups: list[dict] = []
    for row in joined.iter_rows(named=True):
        group_vals = {k: row[k] for k in valid_gb}
        groups.append(
            {
                "group": group_vals,
                "actual": _round(row["actual"]),
                "scenario": _round(row["scenario"]),
                "delta": _round(row["delta"]),
                "delta_pct": _round(row["delta_pct"]),
            }
        )

    total_actual = float(baseline[value_column].cast(pl.Float64).sum())
    total_scenario = float(scenario[value_column].cast(pl.Float64).sum())
    total_delta = total_scenario - total_actual
    total_delta_pct = (
        (total_delta / abs(total_actual) * 100) if total_actual != 0 else None
    )

    return {
        "groups": groups,
        "total_actual": _round(total_actual),
        "total_scenario": _round(total_scenario),
        "total_delta": _round(total_delta),
        "total_delta_pct": _round(total_delta_pct),
    }


def compute_waterfall(
    baseline: pl.DataFrame,
    scenario: pl.DataFrame,
    breakdown_field: str,
    value_column: str = "amount",
) -> list[dict[str, Any]]:
    """Generate waterfall chart data comparing baseline to scenario.

    Returns steps ordered by absolute delta (largest first), suitable for
    a bridge / waterfall chart renderer.  Groups with delta ≈ 0 are omitted.

    Each step::

        {
            "name": str,
            "value": float,          # delta for bridge bars; total for bookends
            "running_total": float,  # cumulative total up to this step
            "is_total": bool         # True for the first and last bookend bars
        }
    """
    if breakdown_field not in baseline.columns or breakdown_field not in scenario.columns:
        logger.warning("breakdown_field %r not in DataFrame", breakdown_field)
        return []

    def _group_sum(df: pl.DataFrame) -> pl.DataFrame:
        return (
            df.lazy()
            .group_by(breakdown_field)
            .agg(pl.col(value_column).cast(pl.Float64).sum().alias("_val"))
            .collect()
        )

    base_grp = _group_sum(baseline)
    scen_grp = _group_sum(scenario)

    joined = base_grp.join(scen_grp, on=breakdown_field, how="outer_coalesce", suffix="_s")
    joined = joined.rename({"_val": "actual", "_val_s": "scenario"})
    joined = joined.with_columns(
        pl.col("actual").fill_null(0.0),
        pl.col("scenario").fill_null(0.0),
    ).with_columns(
        (pl.col("scenario") - pl.col("actual")).alias("delta")
    )

    total_actual = float(baseline[value_column].cast(pl.Float64).sum())
    total_scenario = float(scenario[value_column].cast(pl.Float64).sum())

    # Keep only rows with a meaningful delta (> 0.01% of total or absolute > 1)
    threshold = max(abs(total_actual) * 0.0001, 1.0)
    significant = joined.filter(pl.col("delta").abs() > threshold)
    significant = significant.sort("delta", descending=True)

    steps: list[dict] = []

    # Opening bookend
    steps.append(
        {
            "name": "Actuals",
            "value": _round(total_actual),
            "running_total": _round(total_actual),
            "is_total": True,
        }
    )

    running = total_actual
    for row in significant.iter_rows(named=True):
        delta = float(row["delta"])
        running += delta
        steps.append(
            {
                "name": str(row[breakdown_field]),
                "value": _round(delta),
                "running_total": _round(running),
                "is_total": False,
            }
        )

    # Closing bookend (use actual scenario total, not accumulated running to
    # absorb any floating-point drift)
    steps.append(
        {
            "name": "Scenario",
            "value": _round(total_scenario),
            "running_total": _round(total_scenario),
            "is_total": True,
        }
    )

    return steps


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _round(v: float | None, decimals: int = 2) -> float | None:
    if v is None:
        return None
    return round(float(v), decimals)
