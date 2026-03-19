"""Monthly public dataset exports and uploads to object storage."""

from __future__ import annotations

import argparse
import datetime
import os
import subprocess
from pathlib import Path
from typing import Iterator, Optional

import boto3
from botocore.exceptions import ClientError
import pandas as pd

from agdata.config import CONFIG, get_logger
from agdata.datasets import (
    EXPORT_DATE_FORMAT,
    build_object_key,
    build_versioned_filename,
    get_dataset_by_slug,
    parse_export_date_string,
)
from agdata.dbcon.queries import get_all_latest_descriptions, get_store_apps_metrics

logger = get_logger(__name__)
PUBLIC_BUCKET_NAME = "appgoblin-public"
CHUNK_SIZE = 250000
OUTPUT_DIR = Path("data")
DATASET_EXPORT_CONFIG = {
    "descriptions": {
        "chunk_loader": get_all_latest_descriptions,
        "text_columns_to_sanitize": ["description", "description_short"],
        "date_columns_to_normalize": None,
    },
    "store-apps-metrics": {
        "chunk_loader": get_store_apps_metrics,
        "text_columns_to_sanitize": None,
        "date_columns_to_normalize": [
            "store_last_updated",
            "release_date",
            "appgoblin_updated_at",
        ],
    },
}


def _local_export_path(dataset_slug: str, export_date_string: str) -> Path:
    """Build local output path for a monthly export."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    filename = build_versioned_filename(
        dataset_slug=dataset_slug,
        export_date_string=export_date_string,
    )
    return OUTPUT_DIR / filename


def _sanitize_text_columns(chunk: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Normalize problematic whitespace for TSV exports."""
    for column in columns:
        if column in chunk.columns:
            chunk[column] = (
                chunk[column].astype(str).str.replace("\t", " ", regex=False)
            )
    return chunk


def _normalize_date_columns(chunk: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Normalize datetime columns into date strings for consistency."""
    for column in columns:
        if column in chunk.columns:
            # Only convert if not already datetime
            if chunk[column].dtype == "object":
                chunk[column] = pd.to_datetime(
                    chunk[column], errors="coerce"
                ).dt.strftime("%Y-%m-%d")
            elif (
                hasattr(chunk[column].dtype, "kind") and chunk[column].dtype.kind == "M"
            ):
                # Already datetime, just format
                chunk[column] = chunk[column].dt.strftime("%Y-%m-%d")
    return chunk


def _write_tsv_from_chunks(
    chunk_iterator: Iterator[pd.DataFrame],
    output_path: Path,
    text_columns_to_sanitize: Optional[list[str]] = None,
    date_columns_to_normalize: Optional[list[str]] = None,
) -> Path:
    """Write chunked DataFrame iterator to an uncompressed TSV file."""
    first_chunk = True
    rows_written = 0

    with open(output_path, "w") as f:
        for chunk in chunk_iterator:
            if text_columns_to_sanitize:
                chunk = _sanitize_text_columns(chunk, text_columns_to_sanitize)
            if date_columns_to_normalize:
                chunk = _normalize_date_columns(chunk, date_columns_to_normalize)

            rows_written += len(chunk)
            chunk.to_csv(f, index=False, header=first_chunk, sep="\t")
            first_chunk = False

    logger.info(f"Wrote {rows_written} rows to {output_path}")
    return output_path


def _compress_file_with_xz(input_path: Path) -> Path:
    """Compress a TSV file in place using parallel xz."""
    try:
        subprocess.run(
            ["xz", "-T", "0", "-9", "-f", str(input_path)],
            check=True,
        )
        output_path = input_path.with_suffix(input_path.suffix + ".xz")
        logger.info(f"Compressed {input_path} to {output_path} using parallel xz")
        return output_path
    except subprocess.CalledProcessError as err:
        logger.error(f"Failed to compress {input_path}: {err}")
        raise
    except FileNotFoundError:
        logger.error("xz command not found. Install xz-utils: apt-get install xz-utils")
        raise


def _write_compressed_xz_from_chunks(
    chunk_iterator: Iterator[pd.DataFrame],
    output_path: Path,
    text_columns_to_sanitize: Optional[list[str]] = None,
    date_columns_to_normalize: Optional[list[str]] = None,
) -> Path:
    """Write chunked DataFrame iterator to an XZ-compressed TSV file (two-step process)."""
    temp_tsv_path = output_path.with_suffix("")
    _write_tsv_from_chunks(
        chunk_iterator=chunk_iterator,
        output_path=temp_tsv_path,
        text_columns_to_sanitize=text_columns_to_sanitize,
        date_columns_to_normalize=date_columns_to_normalize,
    )

    compressed_path = _compress_file_with_xz(temp_tsv_path)
    if compressed_path != output_path:
        raise RuntimeError(
            f"Expected compressed file at {output_path}, got {compressed_path}"
        )
    return compressed_path


def get_s3_client() -> boto3.client:
    """Create and return an S3 client."""
    session = boto3.session.Session()
    s3_key = "public-s3"
    return session.client(
        "s3",
        region_name=CONFIG[s3_key]["region_name"],
        endpoint_url="https://" + CONFIG[s3_key]["host"],
        aws_access_key_id=CONFIG[s3_key]["access_key_id"],
        aws_secret_access_key=CONFIG[s3_key]["secret_key"],
    )


def _object_exists(client: boto3.client, bucket: str, key: str) -> bool:
    """Check whether object exists in S3-compatible storage."""
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as err:
        error_code = err.response.get("Error", {}).get("Code")
        if error_code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def publish_public_dataset(
    dataset_slug: str,
    file_path: str,
    export_date_string: str,
    force: bool = False,
) -> bool:
    """
    Publish a dataset file to public S3 storage with canonical naming.

    This is the reusable public publish function for all free datasets.
    It enforces the standard naming pattern and handles object existence checks.

    Args:
        dataset_slug: Canonical dataset slug (e.g., 'store-apps')
        file_path: Local file path to upload
        export_date_string: Export date string in YYYY_MM_DD format
        force: If True, upload even if object already exists. If False, skip if exists.

    Returns:
        True if uploaded, False if skipped (object exists and not forced)

    Raises:
        FileNotFoundError: If local file does not exist
        ValueError: If dataset_slug is not recognized
    """
    # Validate dataset exists
    dataset = get_dataset_by_slug(dataset_slug)
    if not dataset:
        raise ValueError(f"Unknown dataset slug: {dataset_slug}")

    # Check local file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    # Build canonical S3 key
    year, month, _day = parse_export_date_string(export_date_string)
    s3_key = build_object_key(dataset_slug, export_date_string)

    client = get_s3_client()

    # Check idempotency: skip if object exists and not forced
    if not force and _object_exists(client, PUBLIC_BUCKET_NAME, s3_key):
        logger.info(
            f"Skipping {s3_key}: object already exists (use --force to overwrite)"
        )
        return False

    # Upload file with no ACL modifications (assumes bucket-level public access)
    try:
        client.upload_file(
            file_path,
            Bucket=PUBLIC_BUCKET_NAME,
            Key=s3_key,
        )
        logger.info(
            f"Published {dataset_slug} ({year}-{month:02d}): "
            f"s3://{PUBLIC_BUCKET_NAME}/{s3_key}"
        )
        return True
    except Exception as err:
        logger.error(f"Failed to upload {s3_key}: {err}")
        raise


def export_dataset(
    dataset_slug: str,
    export_date_string: str,
    chunksize: int = CHUNK_SIZE,
) -> Path:
    """Export a configured dataset to a versioned, compressed TSV."""
    config = DATASET_EXPORT_CONFIG.get(dataset_slug)
    if config is None:
        raise ValueError(f"Unsupported dataset slug for export: {dataset_slug}")

    output_path = _local_export_path(dataset_slug, export_date_string)
    chunk_iterator = config["chunk_loader"](chunksize=chunksize)
    logger.info(f"Exporting {dataset_slug} to {output_path}")
    return _write_compressed_xz_from_chunks(
        chunk_iterator=chunk_iterator,
        output_path=output_path,
        text_columns_to_sanitize=config["text_columns_to_sanitize"],
        date_columns_to_normalize=config["date_columns_to_normalize"],
    )


def run_dataset_pipeline(
    dataset_slug: str,
    export_date_string: str,
    force: bool = False,
    chunksize: int = CHUNK_SIZE,
) -> bool:
    """Export a dataset and upload it to public storage."""
    local_file = export_dataset(
        dataset_slug=dataset_slug,
        export_date_string=export_date_string,
        chunksize=chunksize,
    )

    return publish_public_dataset(
        dataset_slug=dataset_slug,
        file_path=local_file.as_posix(),
        export_date_string=export_date_string,
        force=force,
    )


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Upload app data exports to public object storage"
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    subparsers.required = True

    # Latest descriptions pipeline
    desc_parser = subparsers.add_parser(
        "descriptions",
        help="Export and upload latest app descriptions",
    )
    desc_parser.add_argument(
        "--force",
        action="store_true",
        help="Force upload even if object already exists",
        default=False,
    )

    metrics_parser = subparsers.add_parser(
        "metrics",
        help="Export and upload store-apps-metrics dataset",
    )
    metrics_parser.add_argument(
        "--force",
        action="store_true",
        help="Force upload even if object already exists",
    )

    monthly_parser = subparsers.add_parser(
        "monthly",
        help="Run all supported datasets for the current month",
    )
    monthly_parser.add_argument(
        "--force",
        action="store_true",
        help="Force upload even if object already exists",
    )

    return parser.parse_args()


def run_all(export_date_string: str, force: bool = False) -> None:
    """Run all supported datasets for the given month."""
    for slug in DATASET_EXPORT_CONFIG:
        logger.info(f"Processing dataset: {slug}")
        try:
            run_dataset_pipeline(
                dataset_slug=slug,
                export_date_string=export_date_string,
                force=force,
            )
        except Exception as err:
            logger.error(f"Error processing {slug}: {err}")


if __name__ == "__main__":
    logger.info("start")
    args = parse_args()
    export_date_string = datetime.datetime.today().date().strftime(EXPORT_DATE_FORMAT)

    if args.command == "descriptions":
        run_dataset_pipeline(
            dataset_slug="descriptions",
            export_date_string=export_date_string,
            force=args.force,
        )
    elif args.command == "metrics":
        run_dataset_pipeline(
            dataset_slug="store-apps-metrics",
            export_date_string=export_date_string,
            force=args.force,
        )
    elif args.command == "monthly":
        run_all(
            export_date_string=export_date_string,
        )
