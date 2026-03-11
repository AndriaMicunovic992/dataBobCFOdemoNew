"""Scenario computation engine using Polars.

Includes both rule-based adjustments (apply_rules) and future-period
projection (generate_projection / apply_scenario_with_projection).

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


def _apply_multiplier_equal(
    lf: pl.LazyFrame, mask: pl.Expr, value_column: str, factor: float
) -> pl.LazyFrame:
    """Apply multiplier as equal distribution: total_delta / row_count added to each matched row."""
    df_full = lf.collect().with_row_index("__row_idx")
    df_match = df_full.filter(mask)

    if df_match.is_empty():
        return df_full.drop("__row_idx").lazy()

    total_abs = float(df_match[value_column].cast(pl.Float64).abs().sum())
    total_delta = total_abs * (factor - 1)
    per_row = total_delta / len(df_match)

    df_match = df_match.with_columns(pl.lit(per_row).alias("_per_row_delta"))
    delta_df = df_match.select(["__row_idx", "_per_row_delta"])
    df_merged = df_full.join(delta_df, on="__row_idx", how="left")
    df_merged = df_merged.with_columns(
        (
            pl.col(value_column).cast(pl.Float64)
            + pl.col("_per_row_delta").fill_null(0.0)
        ).alias(value_column)
    ).drop(["__row_idx", "_per_row_delta"])
    return df_merged.lazy()


def _apply_offset(
    lf: pl.LazyFrame, mask: pl.Expr, value_column: str, total_offset: float,
    distribution: str = "use_base",
) -> pl.LazyFrame:
    """Distribute total_offset across matched rows.

    distribution='use_base' (default): proportional to each row's absolute value.
    distribution='equal': period-aware even split (original behavior).
    """
    # Stamp stable row indices BEFORE filtering so they survive the round-trip.
    df_full = lf.collect().with_row_index("__row_idx")
    df_match = df_full.filter(mask)

    if df_match.is_empty():
        return df_full.drop("__row_idx").lazy()

    if distribution == "use_base":
        # Proportional: each row gets offset * (|row_value| / sum(|all_matched_values|))
        total_abs = float(df_match[value_column].cast(pl.Float64).abs().sum())
        if total_abs == 0:
            # Fallback to equal if all values are 0
            n_rows = len(df_match)
            df_match = df_match.with_columns(
                pl.lit(total_offset / max(n_rows, 1)).alias("_per_row_delta")
            )
        else:
            df_match = df_match.with_columns(
                (pl.col(value_column).cast(pl.Float64).abs() / pl.lit(total_abs) * pl.lit(total_offset))
                .alias("_per_row_delta")
            )
    else:
        # Equal (period-aware even split — original behavior)
        period_col = _find_period_col(df_match)
        if period_col:
            num_periods = df_match[period_col].cast(pl.Utf8).n_unique()
            per_period = total_offset / max(num_periods, 1)
            period_counts = (
                df_match.group_by(period_col)
                .agg(pl.len().alias("_period_row_count"))
            )
            df_match = df_match.join(period_counts, on=period_col, how="left")
            df_match = df_match.with_columns(
                (pl.lit(per_period) / pl.col("_period_row_count")).alias("_per_row_delta")
            ).drop("_period_row_count")
        else:
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

    # Guard: ensure value_column is numeric before attempting scenario math
    col_dtype = df[value_column].dtype
    if col_dtype not in (pl.Float32, pl.Float64, pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64, pl.Decimal):
        numeric_cols = [
            c for c in df.columns
            if df[c].dtype in (pl.Float32, pl.Float64, pl.Int32, pl.Int64)
            and c != "_row_id"
        ]
        raise ValueError(
            f"Column '{value_column}' is not numeric (type: {col_dtype}). "
            f"Available numeric columns: {numeric_cols}"
        )

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
            distribution = rule.get("distribution", "use_base")
            if distribution == "equal":
                lf = _apply_multiplier_equal(lf, mask, value_column, factor)
            else:
                lf = _apply_multiplier(lf, mask, value_column, factor)

        elif rule_type == "offset":
            total_offset = float(rule.get("offset", 0.0))
            distribution = rule.get("distribution", "use_base")
            if total_offset != 0.0:
                lf = _apply_offset(lf, mask, value_column, total_offset, distribution=distribution)

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
# Projection engine (Step 4)
# ---------------------------------------------------------------------------

def _find_period_cols(df: pl.DataFrame) -> list[str]:
    """Return all period-like column names found in the DataFrame."""
    lowered = {c.lower(): c for c in df.columns}
    result = []
    for candidate in _PERIOD_COLUMN_CANDIDATES:
        if candidate in lowered:
            result.append(lowered[candidate])
    return result


def generate_projection(
    df: pl.DataFrame,
    base_config: dict,
    value_column: str = "amount",
) -> pl.DataFrame:
    """Generate projected future-period rows from historical actuals.

    Supported methods:

    * ``copy_year``: Copy rows from ``source_year``, shift their period to
      ``target_year``, and apply an optional ``growth_pct`` multiplier.
      E.g. source_year=2024, target_year=2025, growth_pct=5 → copies Jan–Dec
      2024 rows to Jan–Dec 2025, multiplied by 1.05.

    * ``average``: Average each combination of dimension columns across all
      existing periods, then replicate the averaged row for each period in
      ``target_periods``.  Optional ``growth_pct`` applies on top.

    * ``last_n_months``: Average across the last N distinct periods, then
      replicate for each target period.

    * ``none``: Return an empty DataFrame (no projection rows).

    All projected rows receive ``_data_source = "projected"``; existing rows
    receive ``_data_source = "actual"`` (added by
    ``apply_scenario_with_projection``).

    Returns a DataFrame containing **only** the projected rows.  The caller
    must concat with the original (actual) rows.
    """
    method = base_config.get("method", "none")
    if method == "none":
        return pl.DataFrame()

    period_cols = _find_period_cols(df)
    period_col = period_cols[0] if period_cols else None
    growth_factor = 1.0 + float(base_config.get("growth_pct", 0.0)) / 100.0

    # Identify dimension columns (non-value, non-internal)
    internal = {"_row_id", "_data_source"}
    dim_cols = [
        c for c in df.columns
        if c != value_column and c not in internal and (period_col is None or c != period_col)
    ]

    if method == "copy_year":
        source_year = str(base_config.get("source_year", ""))
        target_year = str(base_config.get("target_year", ""))
        if not source_year or not period_col:
            return pl.DataFrame()

        # Filter to source year rows
        source_df = df.filter(
            pl.col(period_col).cast(pl.Utf8).str.starts_with(source_year)
        )
        if source_df.is_empty():
            return pl.DataFrame()

        # Shift period string: replace leading "source_year" with "target_year"
        projected = source_df.with_columns(
            pl.col(period_col).cast(pl.Utf8)
            .str.replace(source_year, target_year, literal=True)
            .alias(period_col)
        ).with_columns(
            (pl.col(value_column).cast(pl.Float64) * growth_factor).alias(value_column)
        ).with_columns(
            pl.lit("projected").alias("_data_source")
        )
        # Drop internal row IDs — projected rows don't correspond to DB rows
        if "_row_id" in projected.columns:
            projected = projected.drop("_row_id")
        return projected

    elif method in ("average", "last_n_months"):
        target_periods: list[str] = base_config.get("target_periods", [])
        if not target_periods:
            # Auto-generate: next 12 months after last actual period
            if period_col and not df.is_empty():
                last_period = df[period_col].cast(pl.Utf8).drop_nulls().sort().tail(1).item()
                target_periods = _next_periods(last_period, 12)
            else:
                return pl.DataFrame()

        # Select the periods to average over
        if method == "last_n_months" and period_col:
            last_n = int(base_config.get("last_n", 3))
            all_periods = sorted(df[period_col].cast(pl.Utf8).drop_nulls().unique().to_list())
            avg_periods = all_periods[-last_n:] if len(all_periods) >= last_n else all_periods
            source_df = df.filter(pl.col(period_col).cast(pl.Utf8).is_in(avg_periods))
        else:
            source_df = df

        if source_df.is_empty():
            return pl.DataFrame()

        # Average value per dimension combination
        if dim_cols:
            avg_df = (
                source_df.lazy()
                .group_by(dim_cols)
                .agg(pl.col(value_column).cast(pl.Float64).mean().alias(value_column))
                .collect()
            )
        else:
            avg_val = float(source_df[value_column].cast(pl.Float64).mean() or 0.0)
            avg_df = pl.DataFrame({value_column: [avg_val]})

        # Apply growth
        avg_df = avg_df.with_columns(
            (pl.col(value_column) * growth_factor).alias(value_column)
        )

        # Replicate for each target period
        parts = []
        for period in target_periods:
            part = avg_df
            if period_col:
                part = part.with_columns(pl.lit(period).alias(period_col))
            part = part.with_columns(pl.lit("projected").alias("_data_source"))
            parts.append(part)

        return pl.concat(parts, how="diagonal_relaxed") if parts else pl.DataFrame()

    logger.warning("Unknown projection method: %s", method)
    return pl.DataFrame()


def _normalize_base_config(config: dict | None) -> dict:
    """Normalize base_config to the current format, preserving all relevant fields."""
    if not config:
        return {"source": "actuals"}
    return {
        "source": config.get("source", "actuals"),
        "source_scenario_id": config.get("source_scenario_id"),
        "base_year": config.get("base_year"),
        "period_from": config.get("period_from"),
        "period_to": config.get("period_to"),
    }


def _generate_period_range(period_from: str, period_to: str) -> list[str]:
    """Generate all YYYY-MM periods between two endpoints (inclusive)."""
    try:
        y1, m1 = int(period_from[:4]), int(period_from[5:7])
        y2, m2 = int(period_to[:4]), int(period_to[5:7])
    except (ValueError, IndexError):
        return []
    periods = []
    y, m = y1, m1
    while (y, m) <= (y2, m2):
        periods.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
        if len(periods) > 120:  # safety limit: 10 years
            break
    return periods


def _project_periods(
    base_df: pl.DataFrame,
    period_col: str,
    target_periods: list[str],
    value_column: str,
) -> pl.DataFrame:
    """Generate rows for future periods by copying the last baseline period as a template."""
    all_periods = sorted(base_df[period_col].cast(pl.Utf8).drop_nulls().unique().to_list())
    if not all_periods:
        return pl.DataFrame()

    template_period = all_periods[-1]
    template_df = base_df.filter(pl.col(period_col).cast(pl.Utf8) == template_period)
    if template_df.is_empty():
        return pl.DataFrame()

    if "_row_id" in template_df.columns:
        template_df = template_df.drop("_row_id")

    parts = []
    for tp in target_periods:
        shifted = template_df.with_columns(pl.lit(tp).alias(period_col))
        parts.append(shifted)

    return pl.concat(parts, how="diagonal_relaxed") if parts else pl.DataFrame()


def compute_scenario_output(
    actuals_df: pl.DataFrame,
    rules: list[dict],
    base_config: dict | None,
    all_scenarios: dict[str, dict] | None = None,
    value_column: str = "amount",
    _depth: int = 0,
) -> pl.DataFrame:
    """Compute a scenario's output:

    1. Get baseline data (actuals filtered to period range, or another scenario's output).
    2. Apply rules sequentially — rules can target ANY period including future ones.

    For future periods: if a rule targets a period that doesn't exist in the baseline,
    create rows by copying the baseline pattern (same dimension values) into those periods.
    """
    if _depth > 5:
        logger.warning("Scenario chain depth exceeded 5")
        return actuals_df

    config = _normalize_base_config(base_config)
    source = config.get("source", "actuals")
    period_from = config.get("period_from")
    period_to = config.get("period_to")

    # Step 1: Get baseline
    if source == "scenario" and config.get("source_scenario_id") and all_scenarios:
        src = all_scenarios.get(config["source_scenario_id"])
        if src:
            base_df = compute_scenario_output(
                actuals_df, src.get("rules", []), src.get("base_config"),
                all_scenarios, value_column, _depth + 1,
            )
        else:
            base_df = actuals_df
    else:
        base_df = actuals_df

    # Step 2: Filter to base year / period range
    base_year = config.get("base_year")
    period_from = config.get("period_from")
    period_to = config.get("period_to")
    # base_year takes priority; fall back to period_from/period_to for legacy scenarios
    if base_year:
        period_from = f"{base_year}-01"
        period_to = f"{base_year}-12"
    period_col = _find_period_col(base_df)
    if period_col and (period_from or period_to):
        mask = pl.lit(True)
        if period_from:
            mask = mask & (pl.col(period_col).cast(pl.Utf8) >= period_from)
        if period_to:
            mask = mask & (pl.col(period_col).cast(pl.Utf8) <= period_to)
        base_df = base_df.filter(mask)

    # Step 3: Find rule-targeted periods that don't exist in baseline (future periods)
    existing_periods: set[str] = set()
    if period_col and not base_df.is_empty():
        existing_periods = set(base_df[period_col].cast(pl.Utf8).drop_nulls().unique().to_list())

    target_periods: set[str] = set()
    for rule in rules:
        pf = rule.get("periodFrom")
        pt = rule.get("periodTo")
        if pf and pt:
            target_periods.update(_generate_period_range(pf, pt))
        elif pf:
            target_periods.add(pf)
        elif pt:
            target_periods.add(pt)

    future_periods = sorted(target_periods - existing_periods)

    # Step 4: Project into future periods if needed
    if future_periods and period_col and not base_df.is_empty():
        projected = _project_periods(base_df, period_col, future_periods, value_column)
        if not projected.is_empty():
            combined = pl.concat([
                base_df.with_columns(pl.lit("actual").alias("_data_source")),
                projected.with_columns(pl.lit("projected").alias("_data_source")),
            ], how="diagonal_relaxed")
        else:
            combined = base_df.with_columns(pl.lit("actual").alias("_data_source"))
    else:
        combined = base_df.with_columns(pl.lit("actual").alias("_data_source"))

    # Step 5: Apply rules
    return apply_rules(combined, rules, value_column)


def build_scenario_data(
    actuals_df: "pl.DataFrame",
    scenario_rules: list[dict],
    baseline_config: dict | None,
    all_scenarios: dict[str, dict] | None = None,
    value_column: str = "amount",
    _depth: int = 0,
) -> "pl.DataFrame":
    """Build the full scenario output with selectable baseline and optional projection.

    Steps:
    1. Determine baseline (actuals filtered by period, or another scenario's output)
    2. Project into future periods if configured
    3. Apply this scenario's rules

    ``_depth`` prevents infinite recursion in chained scenarios (max 5 levels).
    """
    if _depth > 5:
        logger.warning("Scenario chain depth exceeded 5, stopping recursion")
        return actuals_df

    config = _normalize_base_config(baseline_config)
    source = config.get("source", "actuals")
    period_from = config.get("period_from")
    period_to = config.get("period_to")

    # Step 1: Get base data
    if source == "scenario" and config.get("source_scenario_id") and all_scenarios:
        src_id = config["source_scenario_id"]
        src_sc = all_scenarios.get(src_id)
        if src_sc:
            base_df = build_scenario_data(
                actuals_df, src_sc.get("rules", []), src_sc.get("base_config"),
                all_scenarios, value_column, _depth=_depth + 1,
            )
        else:
            logger.warning("Source scenario %s not found, falling back to actuals", src_id)
            base_df = actuals_df
    else:
        base_df = actuals_df

    # Step 2: Filter to period range if specified
    period_col = _find_period_col(base_df)
    if period_col and (period_from or period_to):
        mask = pl.lit(True)
        if period_from:
            mask = mask & (pl.col(period_col).cast(pl.Utf8) >= period_from)
        if period_to:
            mask = mask & (pl.col(period_col).cast(pl.Utf8) <= period_to)
        base_df = base_df.filter(mask)

    # Step 3: Tag as actual and optionally project to future year
    combined = base_df.with_columns(pl.lit("actual").alias("_data_source"))
    project_to = config.get("project_to_year")
    proj_method = config.get("projection_method", "none")
    if project_to and proj_method != "none":
        # Derive source_year from period_from or first period in data
        source_year: str | None = None
        if period_from and len(period_from) >= 4:
            source_year = period_from[:4]
        elif period_col and not base_df.is_empty():
            first = base_df[period_col].cast(pl.Utf8).drop_nulls().sort().head(1).item()
            source_year = first[:4] if first else None
        if source_year:
            proj_config = {
                "method": "copy_year" if proj_method == "copy" else proj_method,
                "source_year": source_year,
                "target_year": str(project_to),
                "growth_pct": config.get("growth_pct", 0.0),
            }
            projected = generate_projection(base_df, proj_config, value_column)
            if not projected.is_empty():
                combined = pl.concat([combined, projected], how="diagonal_relaxed")

    # Step 4: Apply rules
    result = apply_rules(combined, scenario_rules, value_column)
    return result


def _next_periods(last_period: str, n: int) -> list[str]:
    """Return the next N YYYY-MM periods after last_period."""
    try:
        year, month = int(last_period[:4]), int(last_period[5:7])
    except (ValueError, IndexError):
        return []
    result = []
    for _ in range(n):
        month += 1
        if month > 12:
            month = 1
            year += 1
        result.append(f"{year:04d}-{month:02d}")
    return result


def apply_scenario_with_projection(
    df: pl.DataFrame,
    rules: list[dict],
    base_config: dict | None,
    value_column: str = "amount",
) -> pl.DataFrame:
    """Apply projection (if configured) then scenario rules.

    1. Tag all existing rows as ``_data_source = "actual"``.
    2. Generate projected rows (if base_config.method != "none").
    3. Concatenate actual + projected rows.
    4. Apply scenario rules to the combined DataFrame.

    Returns a DataFrame with an additional ``_data_source`` column.
    """
    # Tag actual rows
    combined = df.with_columns(pl.lit("actual").alias("_data_source"))

    if base_config and base_config.get("method", "none") != "none":
        projected = generate_projection(df, base_config, value_column)
        if not projected.is_empty():
            # Ensure projected has all columns (fill missing with null)
            combined = pl.concat([combined, projected], how="diagonal_relaxed")

    # Apply rules to the combined frame
    result = apply_rules(combined, rules, value_column)
    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _round(v: float | None, decimals: int = 2) -> float | None:
    if v is None:
        return None
    return round(float(v), decimals)
