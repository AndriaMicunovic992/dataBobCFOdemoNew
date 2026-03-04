"""Scenario computation service using Polars."""

# TODO: Implement scenario / what-if computation
# This service should:
# - Accept a dataset_id and a dict of {column: percentage_adjustment}
# - Load the data using Polars (via query service or direct read)
# - Apply percentage adjustments to numeric columns
# - Return both original and adjusted data side-by-side


async def compute_scenario(dataset_id: str, adjustments: dict[str, float], group_by: list[str] | None = None) -> dict:
    """Apply what-if adjustments to a dataset and return comparison.

    TODO: Implement this function.
    - Fetch the dataset table data
    - For each column in adjustments, multiply values by (1 + pct/100)
    - Aggregate by group_by if provided
    - Return {"columns": [...], "original": [...], "scenario": [...]}
    """
    raise NotImplementedError("compute_scenario is not yet implemented")
