import sys
import time
from logging import error, info, basicConfig, getLogger, warning
from os import environ as env
from fire import Fire
from typing import Dict, Tuple, List
from yaml import load, safe_load, YAMLError
from api import ZuoraRevProAPI

import pandas as pd

from api import ZuoraRevProAPI
from google.cloud import storage
from google.oauth2 import service_account
from sqlalchemy.engine.base import Engine


from sheetload.sheetload import gcs_loader
from sheetload_dataframe_utils import dw_uploader
from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    query_executor,
)


zuora_revenue_bi_entity_table_list = [
    {
        "data_entity": "Account Type",
        "physical_tables": "RPRO_BI3_ACCT_TYPE_V",
        "table_name": "BI3_ACCT_TYPE",
    },
]
config_dict = env.copy()
# {
#        "data_entity": "Account Type",
#        "physical_tables": "RPRO_BI3_ACCT_TYPE_V",
#        "table_name": "BI3_ACCT_TYPE",
#    },
#    {
#        "data_entity": "Accounting Pre-Summary",
#        "physical_tables": "RPRO_BI3_RI_ACCT_SUMM_V",
#        "table_name": "BI3_RI_ACCT_SUMM",
#    },
#        "data_entity": "Approvals",
#    {
#        "physical_tables": "RPRO_BI3_APPR_DTL_V",
#        "table_name": "BI3_APPR_DTL",
#    },
#    {
#        "data_entity": "Bill",
#        "physical_tables": "RPRO_BI3_RC_BILL_V",
#        "table_name": "BI3_RC_BILL",
#    },
#    {
#        "data_entity": "Book",
#        "table_name": "BI3_BOOK",
#   },
#        "physical_tables": "RPRO_BI3_BOOK_V",
#    {
#        "data_entity": "Calendar",
#        "physical_tables": "RPRO_BI3_CALENDAR_V",
#        "table_name": "BI3_CALENDAR",
#    },
#    {
#        "data_entity": "Cost",
#        "physical_tables": "RPRO_BI3_RC_LN_COST_V",
#        "table_name": "BI3_RC_LN_COST",
#    },
#    {
#        "data_entity": "Deleted Schedules",
#        "physical_tables": "RPRO_BI3_RC_SCHD_DEL_V",
#        "table_name": "BI3_RC_SCHD_DEL",
#    },
#    {
#        "data_entity": "Header",
#       "table_name": "BI3_RC_HEAD",
#        "physical_tables": "RPRO_BI3_RC_HEAD_V",
#    },
#    {
#        "data_entity": "Holds",
#        "physical_tables": "RPRO_BI3_RC_HOLD_V",
#        "table_name": "BI3_RC_HOLD",
#   },
#    {
#        "data_entity": "Lines",
#     "physical_tables": "RPRO_BI3_RC_LNS_V",
#        "table_name": "BI3_RC_LNS",
#    },
#    {
#        "data_entity": "MJE",
#        "physical_tables": "RPRO_BI3_MJE_V",
#        "table_name": "BI3_MJE",
#    },
#    {
#        "data_entity": "Org",
#        "physical_tables": "RPRO_BI3_ORG_V",
#        "table_name": "BI3_ORG",
#    },
#    {
#        "data_entity": "POB",
#        "physical_tables": "RPRO_BI3_RC_POB_V",
#        "table_name": "BI3_RC_POB",
#    },
#    {
#        "data_entity": "Schedules",
#        "physical_tables": "RPRO_BI3_RC_SCHD_V",
#        "table_name": "BI3_RC_SCHD",
#    },
#    {
#        "data_entity": "Waterfall (Derived)",
#        "physical_tables": "RPRO_BI3_WF_SUMM_V",
#        "table_name": "BI3_WF_SUMM",
#    },
# {
#        "data_entity": "Acct Summary (Derived)",
#        "physical_tables": "RPRO_BI3_LN_ACCT_SUMM_V",
#        "table_name": "BI3_LN_ACCT_SUMM",
#    }"""


def zuora_revenue_extract(
    config_dict: dict, zuora_revenue_bi_entity_table_list: list
) -> None:
    basicConfig(level=20, filename="loging_file.log")

    info("1st Step to get the authentication URL and header informtation.")
    headers = {
        "role": "APIRole",
        "clientname": "Default",
        "Authorization": config_dict["authorization_zuora"],
    }
    authenticate_url_zuora_revpro = config_dict["authenticate_url_zuora_revpro"]
    zuora_fetch_data_url = config_dict["zuora_fetch_data_url"]

    # Initialise the API class
    zuora_revpro = ZuoraRevProAPI()

    for table_name in zuora_revenue_bi_entity_table_list:
        table_name = table_name.get("table_name")
        zuora_revpro.pull_zuora_table_data(
            zuora_fetch_data_url,
            table_name,
            "2001-01-01T00:00:00",
            "2021-04-24T00:00:00",
            "1",
            headers,
            authenticate_url_zuora_revpro,
        )


def zuora_revenue_load(
    bucket: str,
    schema: str,
    table_name: str,
    gapi_keyfile: str = None,
    conn_dict: Dict[str, str] = None,
    compression: str = None,
) -> None:
    # Set some vars
    chunksize = 15000
     chunk_iter = 0
    engine = snowflake_engine_factory(conn_dict or env, "LOADER", schema)

    # Get the gcloud storage client and authenticate
    scope = ["https://www.googleapis.com/auth/cloud-platform"]
    keyfile = load(gapi_keyfile or env["GCP_SERVICE_CREDS"])
    credentials = service_account.Credentials.from_service_account_info(keyfile)
    scoped_credentials = credentials.with_scopes(scope)
    storage_client = storage.Client(credentials=scoped_credentials)
    bucket_obj = storage_client.get_bucket(bucket)

    # Download the file and then pass it in chunks to dw_uploader
    blobs = bucket_obj.list_blobs(prefix=f"RAW_DB/{table_name}")
    for blob in blobs:
        if blob.name.endswith("csv"):
            print(blob.name)
            blob = bucket_obj.blob(blob.name)
            blob.download_to_filename(f"{table_name}.csv")

            try:
                sheet_df = pd.read_csv(
                    f"{table_name}.csv",
                    engine="c",
                    low_memory=False,
                    compression=compression,
                    chunksize=chunksize,
                )
            except FileNotFoundError:
                info("File {} not found.".format(f"{table_name}.csv"))

            for chunk in sheet_df:
                chunk[chunk.columns] = chunk[chunk.columns].astype("str")
                dw_uploader(engine=engine, table=table_name, data=chunk, chunk=chunk_iter)


if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)
    getLogger("snowflake.connector.cursor").disabled = True
    # Copy all environment variables to dict.
    config_dict = env.copy()
    Fire(
        {
            "zuora_extract": zuora_revenue_extract(
                config_dict, zuora_revenue_bi_entity_table_list
            ),
            "zuora_load": zuora_revenue_load,
        }
    )
