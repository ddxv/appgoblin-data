"""Query database for backend API."""

import pathlib
from typing import Iterator, Union

import pandas as pd
from sqlalchemy import text

from agdata.config import MODULE_DIR, get_logger
from agdata.dbcon.connections import get_db_connection

logger = get_logger(__name__)


SQL_DIR = pathlib.Path(MODULE_DIR, "dbcon/sql/")


def load_sql_file(file_name: str) -> str:
    """Load local SQL file based on file name."""
    file_path = pathlib.Path(SQL_DIR, file_name)
    with file_path.open() as file:
        return text(file.read())


QUERY_STORE_APPS = load_sql_file("query_store_apps.sql")
QUERY_STORE_APPS_METRICS = load_sql_file("query_store_apps_metrics.sql")
QUERY_LIVE_STORE_APPS = load_sql_file("query_live_store_apps.sql")
QUERY_APP_DESCRIPTIONS = load_sql_file("query_app_descriptions.sql")


def _read_sql(
    query: str,
    chunksize: Union[int, None] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Read SQL as either a full DataFrame or a chunk iterator."""
    return pd.read_sql(query, con=DBCON.engine, chunksize=chunksize)


def query_store_apps() -> pd.DataFrame:
    """Get all apps and developer info."""
    return _read_sql(QUERY_STORE_APPS)


def query_store_apps_metrics() -> pd.DataFrame:
    """Get all apps metrics and developer info."""
    return _read_sql(QUERY_STORE_APPS_METRICS)


def query_live_store_apps() -> pd.DataFrame:
    """Get all currently live apps and info."""
    return _read_sql(QUERY_LIVE_STORE_APPS)


def get_all_latest_descriptions(
    chunksize: Union[int, None] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Get latest app descriptions as full data or chunks."""
    return _read_sql(QUERY_APP_DESCRIPTIONS, chunksize=chunksize)


def get_store_apps_metrics(
    chunksize: Union[int, None] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Get store apps metrics as full data or chunks."""
    return _read_sql(QUERY_STORE_APPS_METRICS, chunksize=chunksize)


logger.info("set db engine")
DBCON = get_db_connection()
