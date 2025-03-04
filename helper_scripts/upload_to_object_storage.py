from dbcon.queries import get_company_adstxt_publisher_id_apps_raw, query_ad_domains
from config import get_logger, CONFIG
import boto3
import pandas as pd
from typing import Iterator
import os
import argparse

logger = get_logger(__name__)
BUCKET_NAME = "appgoblin-data"
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


def update_company_csv(company_domain: str) -> None:
    """Update company CSV by first writing locally then uploading to S3."""
    client = get_s3_client()
    output_file = "latest.csv"

    try:
        is_first_chunk = True
        with open(output_file, "w") as f:
            try:
                for chunk in get_data_in_chunks(company_domain):
                    chunk.to_csv(f, index=False, header=is_first_chunk)
                    is_first_chunk = False
            except Exception as e:
                logger.error(f"Error writing to file: {str(e)}")
                return

        # Upload the complete file to S3
        s3_key = f"app-ads-txt/domains/domain={company_domain}/latest.csv"
        client.upload_file("latest.csv", Bucket=BUCKET_NAME, Key=s3_key)

    except Exception as e:
        logger.error(f"Error uploading {company_domain}: {str(e)}")


def update_permissions(company_domain: str | None = None) -> None:
    """Update permissions for all companies."""
    if company_domain:
        os.system(
            f"s3cmd setacl s3://{BUCKET_NAME}/app-ads-txt/domains/domain={company_domain}/ --acl-public --recursive"
        )
    else:
        os.system(
            f"s3cmd setacl s3://{BUCKET_NAME}/app-ads-txt/domains/ --acl-public --recursive"
        )


def update_all_company_csvs() -> None:
    """Update CSVs for all companies."""
    ad_domains = query_ad_domains()

    for _, row in ad_domains.iterrows():
        logger.info(f"Updating {row.company_domain}")
        try:
            update_company_csv(row.company_domain)
        except Exception as e:
            logger.error(f"Error processing {row.company_domain}: {str(e)}")

    update_permissions()


def update_single_company_csv(company_domain: str) -> None:
    """Update CSV for a single company."""
    logger.info(f"Starting update for {company_domain}")
    update_company_csv(company_domain)
    logger.info(f"Updated CSV for {company_domain}")
    update_permissions(company_domain)
    logger.info(f"Updated permissions for {company_domain}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--company_domain", type=str, required=False)
    return parser.parse_args()


if __name__ == "__main__":
    logger.info("start")
    args = parse_args()
    if args.company_domain:
        update_single_company_csv(args.company_domain)
    else:
        update_all_company_csvs()
