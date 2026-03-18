from dbcon.queries import get_company_adstxt_publisher_id_apps_raw, query
from config import get_logger, CONFIG
from datasets import build_object_key, get_enabled_datasets, get_dataset_by_slug
import boto3
import pandas as pd
from typing import Iterator, Optional
from datetime import datetime
import os
import subprocess
import argparse

logger = get_logger(__name__)
PUBLIC_BUCKET_NAME = "appgoblin-public"
CHUNK_SIZE = 1000000


def get_data_in_chunks(company_domain: str) -> Iterator[pd.DataFrame]:
    """Generator function to fetch data in chunks."""
    for chunk in get_company_adstxt_publisher_id_apps_raw(
        ad_domain_url=company_domain, chunksize=CHUNK_SIZE
    ):
        yield chunk


def get_s3_client() -> boto3.client:
    """Create and return an S3 client."""
    session = boto3.session.Session()
    return session.client(
        "s3",
        region_name="sgp1",
        endpoint_url="https://sgp1.digitaloceanspaces.com",
        aws_access_key_id=CONFIG["cloud"]["access_key_id"],
        aws_secret_access_key=CONFIG["cloud"]["secret_key"],
    )


def publish_public_dataset(
    dataset_slug: str,
    file_path: str,
    year: int,
    month: int,
    force: bool = False,
) -> bool:
    """
    Publish a dataset file to public S3 storage with canonical naming.

    This is the reusable public publish function for all free datasets.
    It enforces the standard naming pattern and handles object existence checks.

    Args:
        dataset_slug: Canonical dataset slug (e.g., 'store-apps')
        file_path: Local file path to upload
        year: Export year (YYYY)
        month: Export month (MM)
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
    s3_key = build_object_key(dataset_slug, year, month)

    client = get_s3_client()

    # Check idempotency: skip if object exists and not forced
    if not force:
        try:
            client.head_object(Bucket=PUBLIC_BUCKET_NAME, Key=s3_key)
            # Object exists
            logger.info(
                f"Skipping {s3_key}: object already exists (use --force to overwrite)"
            )
            return False
        except client.exceptions.NoSuchKey:
            # Object doesn't exist, proceed with upload
            pass
        except Exception as e:
            # Any other exception (e.g., 404), treat as not exists
            logger.debug(f"Head object check: {e}, proceeding with upload")

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
    except Exception as e:
        logger.error(f"Failed to upload {s3_key}: {e}")
        raise


def get_latest_descriptions_in_chunks() -> Iterator[pd.DataFrame]:
    """Generator function to fetch latest descriptions data in chunks."""
    for chunk in get_all_latest_descriptions(chunksize=CHUNK_SIZE):
        yield chunk


def download_latest_descriptions_tsv() -> None:
    """Download latest descriptions as TSV, stripping tab characters from description fields."""
    output_file = "appgoblin_latest_descriptions.tsv"
    is_first_chunk = True

    with open(output_file, "w") as file_handle:
        for chunk in get_latest_descriptions_in_chunks():
            for description_column in ["description", "description_short"]:
                if description_column in chunk.columns:
                    chunk[description_column] = chunk[description_column].str.replace(
                        "\t", " ", regex=False
                    )

            chunk.to_csv(
                file_handle,
                sep="\t",
                index=False,
                header=is_first_chunk,
            )
            is_first_chunk = False


def compress_latest_descriptions_tsv() -> None:
    """Compress latest descriptions TSV using xz."""
    input_file = "appgoblin_latest_descriptions.tsv"

    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Could not find file to compress: {input_file}")

    subprocess.run(
        [
            "xz",
            "-5",
            "--threads=3",
            "--memlimit-compress=799MiB",
            "--keep",
            "--force",
            input_file,
        ],
        check=True,
    )

    logger.info(f"Compressed {input_file} to {input_file}.xz")


def upload_compressed_latest_descriptions_to_s2(
    year: Optional[int] = None,
    month: Optional[int] = None,
    force: bool = False,
) -> None:
    """
    Upload compressed latest descriptions using the public publish flow.

    Args:
        year: Export year. If None, uses current year.
        month: Export month. If None, uses current month.
        force: If True, upload even if object already exists.
    """
    input_file = "appgoblin_latest_descriptions.tsv.xz"

    if not os.path.exists(input_file):
        raise FileNotFoundError(
            f"Could not find compressed file to upload: {input_file}"
        )

    # Determine publication date
    if year is None or month is None:
        now = datetime.now()
        year = year or now.year
        month = month or now.month

    # Use the reusable public publish function
    publish_public_dataset(
        dataset_slug="descriptions",
        file_path=input_file,
        year=year,
        month=month,
        force=force,
    )


def run_latest_descriptions_pipeline(
    year: Optional[int] = None,
    month: Optional[int] = None,
    force: bool = False,
) -> None:
    """
    Run latest descriptions export pipeline: download, compress, and upload.

    Args:
        year: Export year. If None, uses current year.
        month: Export month. If None, uses current month.
        force: If True, upload even if object already exists.
    """
    logger.info("Starting latest descriptions pipeline")
    download_latest_descriptions_tsv()
    compress_latest_descriptions_tsv()
    upload_compressed_latest_descriptions_to_s2(year=year, month=month, force=force)
    logger.info("Completed latest descriptions pipeline")


def run_monthly_exports(
    year: int,
    month: int,
    dataset_slugs: Optional[list[str]] = None,
    force: bool = False,
) -> dict[str, bool]:
    """
    Run all enabled datasets (or specified datasets) for a target month.

    This is the monthly batch export mode - idempotent by default, skipping
    objects that already exist unless --force is specified.

    Args:
        year: Export year (YYYY)
        month: Export month (MM, 1-12)
        dataset_slugs: List of dataset slugs to export. If None, exports all enabled datasets.
        force: If True, upload even if object already exists.

    Returns:
        Dictionary mapping dataset slug to success status (True=uploaded, False=skipped/exists)

    Raises:
        ValueError: If month is out of range (1-12)
    """
    if not 1 <= month <= 12:
        raise ValueError(f"Month must be 1-12, got {month}")

    logger.info(f"Starting monthly export for {year}-{month:02d}")

    # Determine which datasets to process
    if dataset_slugs:
        datasets_to_run = []
        for slug in dataset_slugs:
            dataset = get_dataset_by_slug(slug)
            if not dataset:
                logger.warning(f"Unknown dataset slug: {slug}, skipping")
                continue
            datasets_to_run.append(dataset)
    else:
        datasets_to_run = get_enabled_datasets()

    results = {}

    # Run each dataset
    for dataset in datasets_to_run:
        try:
            logger.info(f"Processing dataset: {dataset.slug}")

            # TODO: Call appropriate export function for each dataset
            # For now, only descriptions is implemented
            if dataset.slug == "descriptions":
                run_latest_descriptions_pipeline(
                    year=year,
                    month=month,
                    force=force,
                )
                results[dataset.slug] = True
            else:
                logger.warning(
                    f"Export not yet implemented for {dataset.slug}, skipping"
                )
                results[dataset.slug] = False

        except Exception as e:
            logger.error(f"Failed to export {dataset.slug}: {e}", exc_info=True)
            results[dataset.slug] = False

    # Summary
    successful = sum(1 for v in results.values() if v)
    logger.info(
        f"Monthly export complete: {successful}/{len(results)} datasets processed"
    )

    return results


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Upload app data exports to public object storage"
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # Latest descriptions pipeline
    desc_parser = subparsers.add_parser(
        "descriptions",
        help="Export and upload latest app descriptions",
    )
    desc_parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Export year (default: current year)",
    )
    desc_parser.add_argument(
        "--month",
        type=int,
        default=None,
        help="Export month (default: current month)",
    )
    desc_parser.add_argument(
        "--force",
        action="store_true",
        help="Force upload even if object already exists",
    )

    # Monthly export command
    monthly_parser = subparsers.add_parser(
        "monthly",
        help="Run all enabled datasets for a target month",
    )
    monthly_parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Export year (YYYY)",
    )
    monthly_parser.add_argument(
        "--month",
        type=int,
        required=True,
        help="Export month (MM, 1-12)",
    )
    monthly_parser.add_argument(
        "--datasets",
        nargs="+",
        default=None,
        help="Specific datasets to export (default: all enabled). "
        "Use dataset slugs (e.g., store-apps descriptions)",
    )
    monthly_parser.add_argument(
        "--force",
        action="store_true",
        help="Force upload even if object already exists",
    )

    return parser.parse_args()


if __name__ == "__main__":
    logger.info("start")
    args = parse_args()

    if args.command == "descriptions":
        run_latest_descriptions_pipeline(
            year=args.year,
            month=args.month,
            force=args.force,
        )
    elif args.command == "monthly":
        run_monthly_exports(
            year=args.year,
            month=args.month,
            dataset_slugs=args.datasets,
            force=args.force,
        )
    else:
        logger.error(f"Unknown command: {args.command}")
