"""Query database for backend API."""

import datetime
import pathlib
from functools import lru_cache

import pandas as pd
from sqlalchemy import text

from config import MODULE_DIR, get_logger
from dbcon.connections import get_db_connection

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
QUERY_APPS_COMPANIES = load_sql_file(
    "query_store_apps_companies.sql",
)


def query_store_apps() -> pd.DataFrame:
    """Get all apps and developer info."""
    df = pd.read_sql(QUERY_STORE_APPS, con=DBCON.engine)
    return df

def query_store_apps_companies() -> pd.DataFrame:
    """Get all apps and developer info."""
    df = pd.read_sql(QUERY_APPS_COMPANIES, con=DBCON.engine)
    return df



logger.info("set db engine")
DBCON = get_db_connection("madrone")
DBCON.set_engine()
