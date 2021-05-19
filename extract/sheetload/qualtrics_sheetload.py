from logging import error, info, basicConfig, getLogger, warning
from os import environ as env

from gitlabdata.orchestration_utils import (
    postgres_engine_factory,
    snowflake_engine_factory,
    query_executor,
)
from google_sheets_client import APIError, GoogleSheetsClient
from qualtrics_client import QualtricsClient
from sheetload_dataframe_utils import dw_uploader


def construct_qualtrics_contact(result):
    return {
        "firstName": result["first_name"],
        "lastName": result["last_name"],
        "email": result["email_address"],
        "language": result["language"],
        "embeddedData": {
            "gitlabUserID": result["user_id"],
            "user_id": result["user_id"],
            "plan": result["plan"],
            "namespace_id": result["namespace_id"],
        },
    }


def get_qualtrics_request_table_name(file_id):
    return "".join(x for x in file_id if x.isalpha()).lower()


def should_file_be_processed(file_name, tab, qualtrics_mailing_lists):
    _, file_name_split = file_name.split(".")
    if tab in qualtrics_mailing_lists:
        info(
            f"{file_name}: Qualtrics already has mailing list with corresponding name -- not processing."
        )
        return False
    if tab != file_name_split:
        error(
            f"{file_name}: First worksheet did not match expected name of {file_name_split}"
        )
        return False
    return True


def push_contacts_to_qualtrics(
    tab_name, file, qualtrics_client, qualtrics_contacts
) -> str:
    final_status = "processed"
    try:
        mailing_id = qualtrics_client.create_mailing_list(
            env["QUALTRICS_POOL_ID"], tab_name, env["QUALTRICS_GROUP_ID"]
        )
    except:
        file.sheet1.update_acell(
            "A1",
            "Mailing list could not be created in Qualtrics.  Try changing mailing list name.",
        )
        raise
    else:
        error_contacts = qualtrics_client.upload_contacts_to_mailing_list(
            env["QUALTRICS_POOL_ID"], mailing_id, qualtrics_contacts
        )
        error_contacts_ids = [
            contact["embeddedData"]["gitlabUserID"] for contact in error_contacts
        ]
        if error_contacts_ids:
            final_status = f"{final_status} except {error_contacts_ids}"
    return final_status


def get_metadata(
    file,
    google_sheet_client,
    maximum_backoff_sec: int = 300,
):
    """
    Returns the google sheet name and file name
    """
    n = 0
    while maximum_backoff_sec > (2 ** n):
        try:
            file_name = file.title
            tab = file.sheet1.title
            return file_name, tab
        except APIError as gspread_error:
            if gspread_error.response.status_code in (429, 500, 502, 503):
                google_sheet_client.wait_exponential_backoff(n)
                n = n + 1
            else:
                raise
    else:
        error(f"Max retries exceeded, giving up on {file}")
        return


def process_qualtrics_file(
    file,
    is_test,
    google_sheet_client,
    schema,
    qualtrics_client,
    qualtrics_mailing_lists,
):
    file_name, tab = get_metadata(file, google_sheet_client)
    if not should_file_be_processed(file_name, tab, qualtrics_mailing_lists):
        return

    dataframe = google_sheet_client.load_google_sheet(None, file_name, tab)
    if list(dataframe.columns.values)[0].lower() != "id":
        info(f"{file.title}: First column did not match expected name of id")
        return
    if not is_test:
        file.sheet1.update_acell("A1", "processing")
    engine = snowflake_engine_factory(env, "LOADER", schema)
    analytics_engine = snowflake_engine_factory(env, "CI_USER")
    table = get_qualtrics_request_table_name(file.id)
    dw_uploader(engine, table, dataframe, schema)
    query = f"""
        SELECT first_name, last_name, email_address, language, user_id, plan, namespace_id
        FROM PREP.SENSITIVE.QUALTRICS_API_FORMATTED_CONTACTS 
        WHERE user_id in
        (
            SELECT id
            FROM RAW.{schema}.{table}
            WHERE TRY_TO_NUMBER(id) IS NOT NULL
        )
    """
    results = []
    if not is_test:
        results = query_executor(analytics_engine, query)

    qualtrics_contacts = [construct_qualtrics_contact(result) for result in results]

    if is_test:
        info(f"Not updating file for test.")
    else:
        final_status = push_contacts_to_qualtrics(
            tab, file, qualtrics_client, qualtrics_contacts
        )
        file.client.login()
        file.sheet1.update_acell("A1", final_status)


def qualtrics_loader(load_type: str):
    is_test = load_type == "test"
    google_sheet_client = GoogleSheetsClient()
    prefix = "qualtrics_mailing_list."
    if is_test:
        prefix = "test_" + prefix
    all_qualtrics_files_to_load = [
        file
        for file in google_sheet_client.get_visible_files()
        if file.title.lower().startswith(prefix)
    ]

    schema = "qualtrics_mailing_list"

    if not is_test:
        qualtrics_client = QualtricsClient(
            env["QUALTRICS_API_TOKEN"], env["QUALTRICS_DATA_CENTER"]
        )

        qualtrics_mailing_lists = [
            mailing_list for mailing_list in qualtrics_client.get_mailing_lists()
        ]

    else:
        qualtrics_client = None
        qualtrics_mailing_lists = []

    for file in all_qualtrics_files_to_load:
        process_qualtrics_file(
            file,
            is_test,
            google_sheet_client,
            schema,
            qualtrics_client,
            qualtrics_mailing_lists,
        )
