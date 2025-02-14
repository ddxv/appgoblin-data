import io
from dbcon.queries import get_company_adstxt_publisher_id_apps_raw, query_ad_domains
from config import get_logger, CONFIG
import boto3
import pandas as pd
from typing import Iterator

logger = get_logger(__name__)
BUCKET_NAME = "appgoblin-data"
CHUNK_SIZE = 1000000


def get_data_in_chunks(company_domain: str) -> Iterator[pd.DataFrame]:
    """Generator function to fetch data in chunks."""
    for chunk in get_company_adstxt_publisher_id_apps_raw(
        ad_domain_url=company_domain, chunksize=CHUNK_SIZE
    ):
        yield chunk


def upload_to_s3(
    client: boto3.client, data: str, company_domain: str, is_first_chunk: bool = False
) -> bool:
    """Upload data to S3 with append functionality."""
    try:
        s3_key = f"app-ads-txt/domains/domain={company_domain}/latest.csv"

        # For first chunk, use put_object to create/overwrite file
        if is_first_chunk:
            response = client.put_object(Body=data, Bucket=BUCKET_NAME, Key=s3_key)
        # For subsequent chunks, use append operation
        else:
            # Get existing object
            existing_obj = client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
            # Append new data
            updated_data = existing_obj["Body"].read().decode("utf-8") + data

            response = client.put_object(
                Body=updated_data, Bucket=BUCKET_NAME, Key=s3_key
            )

        return response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200
    except Exception as e:
        logger.error(f"Error uploading to S3: {str(e)}")
        return False


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
    """Update company CSV in chunks to avoid memory issues."""
    client = get_s3_client()
    is_first_chunk = True

    try:
        for chunk in get_data_in_chunks(company_domain):
            # Convert chunk to CSV string
            csv_buffer = io.StringIO()
            chunk.to_csv(csv_buffer, index=False, header=is_first_chunk)

            # Upload chunk
            success = upload_to_s3(
                client, csv_buffer.getvalue(), company_domain, is_first_chunk
            )

            if not success:
                logger.error(f"Failed to upload chunk for {company_domain}")
                return

            is_first_chunk = False

    except Exception as e:
        logger.error(f"Error processing {company_domain}: {str(e)}")


def update_all_company_csvs() -> None:
    """Update CSVs for all companies."""
    ad_domains = query_ad_domains()

    for _, row in ad_domains.iterrows():
        logger.info(f"Updating {row.company_domain}")
        update_company_csv(row.company_domain)


if __name__ == "__main__":
    logger.info("start")
    update_all_company_csvs()
