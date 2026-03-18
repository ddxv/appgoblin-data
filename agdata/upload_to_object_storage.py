from dbcon.queries import get_company_adstxt_publisher_id_apps_raw, query_ad_domains
from config import get_logger, CONFIG
import boto3
import pandas as pd
from typing import Iterator
import os
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


def upload_compressed_latest_descriptions_to_s2() -> None:
    """Upload compressed latest descriptions TSV archive to S2."""
    input_file = "appgoblin_latest_descriptions.tsv.xz"
    dataset_path = "downloads/free/latest-descriptions/"

    if not os.path.exists(input_file):
        raise FileNotFoundError(
            f"Could not find compressed file to upload: {input_file}"
        )

    client = get_s3_client()
    s3_key = f"{dataset_path}{input_file}"
    client.upload_file(input_file, Bucket=PUBLIC_BUCKET_NAME, Key=s3_key)
    # update_permissions(dataset_path)

    logger.info(f"Uploaded {input_file} to s3://{PUBLIC_BUCKET_NAME}/{s3_key}")


def run_latest_descriptions_pipeline() -> None:
    """Run latest descriptions export pipeline: download, compress, and upload."""
    logger.info("Starting latest descriptions pipeline")
    download_latest_descriptions_tsv()
    compress_latest_descriptions_tsv()
    upload_compressed_latest_descriptions_to_s2()
    logger.info("Completed latest descriptions pipeline")


if __name__ == "__main__":
    logger.info("start")
    args = parse_args()
    if args.company_domain:
        update_single_company_csv(args.company_domain)
    else:
        update_all_company_csvs()
