"""Calendar dimension service.

Creates a permanent _calendar dataset (2020-01-01 → 2027-12-31) and
auto-links any time-role column in newly uploaded datasets to it.
"""
from __future__ import annotations

import re
import uuid
import logging
from datetime import date, timedelta

import polars as pl
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import sync_engine, AsyncSessionLocal
from app.models.metadata import Dataset, DatasetColumn, DatasetRelationship
from app.services import storage as storage_svc

logger = logging.getLogger(__name__)

_CAL_NAME = "_calendar"
_CAL_TABLE = "dim_calendar"
_START = date(2020, 1, 1)
_END   = date(2027, 12, 31)

_MONTHS = ["January","February","March","April","May","June",
           "July","August","September","October","November","December"]

_COLUMNS_META = [
    {"column_name": "date",       "data_type": "text", "column_role": "time"},
    {"column_name": "month_year", "data_type": "text", "column_role": "attribute"},
    {"column_name": "year",       "data_type": "text", "column_role": "attribute"},
    {"column_name": "month",      "data_type": "text", "column_role": "attribute"},
    {"column_name": "month_name", "data_type": "text", "column_role": "attribute"},
    {"column_name": "quarter",    "data_type": "text", "column_role": "attribute"},
]


def _build_calendar_df() -> pl.DataFrame:
    dates, month_years, years, months, month_names, quarters = [], [], [], [], [], []
    cur = _START
    while cur <= _END:
        dates.append(cur.isoformat())
        month_years.append(f"{cur.year:04d}-{cur.month:02d}")
        years.append(str(cur.year))
        months.append(f"{cur.month:02d}")
        month_names.append(_MONTHS[cur.month - 1])
        q = (cur.month - 1) // 3 + 1
        quarters.append(f"Q{q} {cur.year}")
        cur += timedelta(days=1)
    return pl.DataFrame({
        "date": dates, "month_year": month_years, "year": years,
        "month": months, "month_name": month_names, "quarter": quarters,
    })


async def ensure_calendar() -> None:
    """Idempotent: create the _calendar dataset if it doesn't already exist."""
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Dataset).where(Dataset.name == _CAL_NAME, Dataset.status != "deleted")
        )
        if result.scalar_one_or_none():
            logger.info("Calendar dataset already exists, skipping seed")
            return

        try:
            df = _build_calendar_df()
            storage_svc.create_dataset_table(sync_engine, _CAL_TABLE, _COLUMNS_META)
            row_count = storage_svc.load_data(sync_engine, _CAL_TABLE, df, _COLUMNS_META)

            ds = Dataset(
                id=uuid.uuid4().hex,
                model_id=None,  # global — visible in all models
                name=_CAL_NAME,
                table_name=_CAL_TABLE,
                source_filename=None,
                row_count=row_count,
                status="active",
                ai_analyzed=True,
                ai_notes={
                    "is_system": True,
                    "description": "Auto-generated calendar dimension (2020-2027). "
                                   "Provides date, month_year, year, month, quarter columns.",
                },
            )
            db.add(ds)
            for col_meta in _COLUMNS_META:
                db.add(DatasetColumn(
                    id=uuid.uuid4().hex,
                    dataset_id=ds.id,
                    column_name=col_meta["column_name"],
                    display_name=col_meta["column_name"].replace("_", " ").title(),
                    data_type=col_meta["data_type"],
                    column_role=col_meta["column_role"],
                    unique_count=len(df[col_meta["column_name"]].unique()),
                ))
            await db.commit()
            logger.info("Calendar dataset created (%d rows)", row_count)
        except Exception:
            logger.exception("Failed to create calendar dataset")


def _detect_cal_col(sample_values: list) -> str | None:
    """Return 'date' for YYYY-MM-DD samples, 'month_year' for YYYY-MM, else None."""
    samples = [str(v) for v in sample_values if v is not None][:10]
    if any(re.match(r"^\d{4}-\d{2}-\d{2}$", s) for s in samples):
        return "date"
    if any(re.match(r"^\d{4}-\d{2}$", s) for s in samples):
        return "month_year"
    return None


async def auto_link_calendar(db: AsyncSession, dataset: Dataset) -> None:
    """Create DatasetRelationship from each time-role column → calendar."""
    result = await db.execute(
        select(Dataset).where(Dataset.name == _CAL_NAME, Dataset.status != "deleted")
    )
    cal = result.scalar_one_or_none()
    if cal is None:
        return

    for col in dataset.columns:
        if col.column_role != "time":
            continue
        cal_col = _detect_cal_col(col.sample_values or [])
        if not cal_col:
            continue
        # Skip if relationship already exists
        existing = await db.execute(
            select(DatasetRelationship).where(
                DatasetRelationship.source_dataset_id == dataset.id,
                DatasetRelationship.source_column == col.column_name,
                DatasetRelationship.target_dataset_id == cal.id,
            )
        )
        if existing.scalar_one_or_none():
            continue
        db.add(DatasetRelationship(
            id=uuid.uuid4().hex,
            source_dataset_id=dataset.id,
            target_dataset_id=cal.id,
            source_column=col.column_name,
            target_column=cal_col,
        ))
    await db.commit()
