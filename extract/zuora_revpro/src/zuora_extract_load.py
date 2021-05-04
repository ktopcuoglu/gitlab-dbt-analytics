import logging
import sys
import time
from os import environ as env
from fire import Fire

from api import ZuoraRevProAPI

zuora_revenue_bi_entity_table_list = [
    {
        "data_entity": "Acct Summary (Derived)",
        "physical_tables": "RPRO_BI3_LN_ACCT_SUMM_V",
        "table_name": "BI3_LN_ACCT_SUMM",
    }
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
#    },"""


def zuora_revenue_extract(
    config_dict: dict, zuora_revenue_bi_entity_table_list: list
) -> None:

    logging.info("1st Step to get the authentication URL and header informtation.")
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
        url = f"{zuora_fetch_data_url}{table_name}/describe-columns"
        header_token = zuora_revpro.get_auth_token(
            authenticate_url_zuora_revpro=authenticate_url_zuora_revpro, headers=headers
        )
        header_auth_token = {"Token": header_token}
        logging.info(
            f"Fetch the table description of and generate the DDL for RAW schema for table {table_name}"
        )
        zuora_revpro.zuora_table_desc(url, header_auth_token, table_name)
        time.sleep(30)

        zuora_revpro.pull_zuora_table_data(
            zuora_fetch_data_url,
            table_name,
            "2001-01-01T00:00:00",
            "2021-04-24T00:00:00",
            "1",
            headers,
            authenticate_url_zuora_revpro,
        )


def zuora_revenue_load()->None:
    print("zuora_revenue_load")


if __name__ == "__main__":
    logging.basicConfig(level=20, filename="loging_file.log")
    # Copy all environment variables to dict.
    config_dict = env.copy()
    Fire({"zuora_extract": zuora_revenue_extract(config_dict,zuora_revenue_bi_entity_table_list), "zuora_load": zuora_revenue_load})
