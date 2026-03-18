"""Query database for backend API."""

import pathlib
from typing import Iterator, Union
import numpy as np

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


QUERY_STORE_APPS = load_sql_file(
    "query_store_apps.sql",
)
QUERY_STORE_APPS_METRICS = load_sql_file(
    "query_store_apps_metrics.sql",
)
QUERY_APPS_COMPANIES = load_sql_file(
    "query_store_apps_companies.sql",
)
QUERY_LIVE_STORE_APPS = load_sql_file(
    "query_live_store_apps.sql",
)


def query_store_apps() -> pd.DataFrame:
    """Get all apps and developer info."""
    df = pd.read_sql(QUERY_STORE_APPS, con=DBCON.engine)
    return df


def query_store_apps_metrics() -> pd.DataFrame:
    """Get all apps and developer info."""
    df = pd.read_sql(QUERY_STORE_APPS_METRICS, con=DBCON.engine)
    return df


def query_store_apps_companies() -> pd.DataFrame:
    """Get all apps and developer info."""
    df = pd.read_sql(QUERY_APPS_COMPANIES, con=DBCON.engine)
    return df


def query_live_store_apps() -> pd.DataFrame:
    """Get all apps and developer info."""
    df = pd.read_sql(QUERY_LIVE_STORE_APPS, con=DBCON.engine)
    return df


def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process a DataFrame chunk with the required transformations.
    """
    # Create a copy to avoid modifying the original DataFrame
    df = df.copy()

    # Apply transformations
    df["store"] = df["store"].replace({1: "Android", 2: "iOS"})
    df["developer_id"] = df["developer_id"].astype(str)

    # Use numpy.select for more efficient conditional logic
    conditions = [df["store"] == "Android"]
    choices = ["https://play.google.com/store/apps/details?id=" + df["store_id"]]
    default = "https://apps.apple.com/-/app/-/id" + df["store_id"]

    df["store_url"] = np.select(conditions, choices, default=default)

    return df


def get_all_latest_descriptions(
    chunksize: Union[int, None] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Get all latest descriptions for all apps with optional chunked processing."""
    sel_query = """WITH latest_descriptions AS (
    SELECT DISTINCT ON (sad.store_app)
        sad.id AS description_id,
        sad.store_app,
        sad.description_short,
        sad.description,
        sad.updated_at AS description_last_updated
    FROM
        store_apps_descriptions AS sad
    WHERE
        sad.language_id = 1
    ORDER BY
        sad.store_app ASC,
        sad.updated_at DESC
    )
    SELECT CASE WHEN sa.store = 1 THEN 'Android' ELSE 'iOS' END AS appstore, sa.store_id, sa.category, ld.description_short, ld.description, ld.description_last_updated from latest_descriptions ld
    LEFT JOIN frontend.store_apps_overview sa ON ld.store_app = sa.id
    ;"""
    df_or_iterator = pd.read_sql(
        sel_query,
        con=DBCON.engine,
        chunksize=chunksize,
    )

    if chunksize is None:
        return df_or_iterator

    return df_or_iterator


logger.info("set db engine")
DBCON = get_db_connection()
DBCON.set_engine()
