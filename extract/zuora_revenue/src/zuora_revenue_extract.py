import argparse
import sys
import logging
from api import ZuoraRevProAPI

# Define the argument required to run the  extraction process
parser = argparse.ArgumentParser(
    description="This enable to run the Extraction process for one table at a time."
)
parser.add_argument(
    "-table_name",
    action="store",
    dest="table_name",
    required=True,
    help="Provide table_name or Name of the BI view.",
)
parser.add_argument(
    "-bucket_name",
    action="store",
    dest="bucket_name",
    required=True,
    help="Provide the bucket Name where the file needs to be stored.",
)
parser.add_argument(
    "-api_dns_name",
    action="store",
    dest="api_dns_name",
    required=True,
    help="API dns name.",
)
parser.add_argument(
    "-api_auth_code",
    action="store",
    dest="api_auth_code",
    required=True,
    help="Authorization code for connection.",
)
parser.add_argument(
    "-from_date",
    action="store",
    dest="from_date",
    required=False,
    help="The date from which the data query begins.Format date YYYY-MM-DDTHH:MM:SS",
)
results = parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(
        filename="zuora_extract.log",
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=20,
    )
    logger = logging.getLogger(__name__)
    logger.info("Prepare the URL for data extraction and authentication")
    config_dict = {
        "headers": {
            "role": "APIRole",
            "clientname": "Default",
            "Authorization": results.api_auth_code,
        },
        "authenticate_url_zuora_revpro": (
            "https://" + results.api_dns_name + "/api/integration/v1/authenticate"
        ),
        "zuora_fetch_data_url": (
            "https://" + results.api_dns_name + "/api/integration/v2/biviews/"
        ),
        "bucket_name": results.bucket_name,
        "clientId": "1",
    }

    # Initialise the API class
    zuora_revpro = ZuoraRevProAPI(config_dict)
    # Fetch the start and end date for table query.
    from_date, end_date = zuora_revpro.date_range(results.from_date)
    logger.info(f"The date range for extraction is {from_date} to {end_date}")
    # Pull the data for the BI view for defined start and end date
    zuora_revpro.pull_zuora_table_data(
        results.table_name,
        from_date,
        end_date,
    )
    print(f"{results.table_name} = end_date")
    