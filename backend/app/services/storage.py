"""Dynamic table creation and data loading service."""

# TODO: Implement dynamic table creation and data loading
# This service should:
# - Receive ParsedSheet objects from the parser
# - Dynamically create PostgreSQL tables using SQLAlchemy Core (sync engine)
# - Load data from Polars DataFrames into those tables efficiently
# - Store dataset metadata in the datasets / dataset_columns tables


async def create_dataset_table(table_name: str, columns: list) -> None:
    """Dynamically create a PostgreSQL table for a dataset.

    TODO: Implement this function.
    - Use sync_engine with SQLAlchemy Table / Column objects
    - Map Polars dtypes to SQLAlchemy types
    - Use IF NOT EXISTS to be idempotent
    """
    raise NotImplementedError("create_dataset_table is not yet implemented")


async def load_data(table_name: str, file_path: str, sheet_name: str | None = None) -> int:
    """Load data from a file into the dynamic table.

    TODO: Implement this function.
    - Read the file with Polars
    - Use bulk insert via psycopg2 COPY or SQLAlchemy bulk_insert_mappings
    - Return the number of rows inserted
    """
    raise NotImplementedError("load_data is not yet implemented")
