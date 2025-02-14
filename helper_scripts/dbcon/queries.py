"""Query database for backend API."""

import pathlib

import numpy as np

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
QUERY_LIVE_STORE_APPS = load_sql_file(
    "query_live_store_apps.sql",
)
QUERY_COMPANY_ADSTXT_PUBLISHER_ID = load_sql_file(
    "query_company_adstxt_publisher_id.sql"
)
QUERY_AD_DOMAINS = load_sql_file("query_ad_domains.sql")


def query_store_apps() -> pd.DataFrame:
    """Get all apps and developer info."""
    df = pd.read_sql(QUERY_STORE_APPS, con=DBCON.engine)
    return df


def query_store_apps_companies() -> pd.DataFrame:
    """Get all apps and developer info."""
    df = pd.read_sql(QUERY_APPS_COMPANIES, con=DBCON.engine)
    return df


def query_live_store_apps() -> pd.DataFrame:
    """Get all apps and developer info."""
    df = pd.read_sql(QUERY_LIVE_STORE_APPS, con=DBCON.engine)
    return df


def query_ad_domains() -> pd.DataFrame:
    """Get all ad domains"""
    df = pd.read_sql(QUERY_AD_DOMAINS, con=DBCON.engine)
    return df


def get_company_adstxt_publisher_id_apps_raw(
    ad_domain_url: str,
) -> pd.DataFrame:
    """Get ad domain publisher id."""
    df = pd.read_sql(
        QUERY_COMPANY_ADSTXT_PUBLISHER_ID,
        DBCON.engine,
        params={
            "ad_domain_url": ad_domain_url,
        },
    )
    df["store"] = df["store"].replace({1: "Android", 2: "iOS"})
    df["developer_id"] = df["developer_id"].astype(str)
    df["store_url"] = np.where(
        df["store"] == "Android",
        "https://play.google.com/store/apps/details?id=" + df["store_id"],
        "https://apps.apple.com/-/app/-/id" + df["store_id"],
    )
    return df


logger.info("set db engine")
DBCON = get_db_connection(use_ssh_tunnel=False)
DBCON.set_engine()
