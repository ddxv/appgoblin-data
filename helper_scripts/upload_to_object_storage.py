import io
from dbcon.queries import get_company_adstxt_publisher_id_apps_raw, query_ad_domains

from config import get_logger, CONFIG

import boto3

logger = get_logger(__name__)

BUCKET_NAME = "appgoblin-data"


def update_all_company_csvs() -> None:
    ad_domains = query_ad_domains()

    for row in ad_domains.iterrows():
        company_domain = row.company_domain
        update_company_csv(company_domain)


def update_company_csv(company_domain: str) -> None:
    df = get_company_adstxt_publisher_id_apps_raw(ad_domain_url=company_domain)

    secret_key = CONFIG["cloud"]["secret_key"]
    access_key_id = CONFIG["cloud"]["access_key_id"]

    session = boto3.session.Session()
    client = session.client(
        "s3",
        region_name="sgp1",
        endpoint_url="https://sgp1.digitaloceanspaces.com",
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_key,
    )

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    # Put the CSV string to S3
    response = client.put_object(
        Body=csv_buffer.getvalue(),
        Bucket=BUCKET_NAME,
        Key="app-ads-txt/domains/domain={}/latest.csv".format(company_domain),
    )

    if response.get("ResponseMetadata").get("HTTPStatusCode") == 200:
        pass
    else:
        logger.error("Failed to upload {company_domain} to S3")


if __name__ == "__main__":
    logger.info("start")
    update_all_company_csvs()
