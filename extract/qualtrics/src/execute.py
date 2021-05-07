from datetime import datetime, timedelta
import json
from os import environ as env

from typing import Any, Dict, List, Generator

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)
from qualtrics_client import QualtricsClient


def timestamp_in_interval(tstamp: datetime, start: datetime, end: datetime) -> bool:
    """
    Returns true if tstamp is in the interval [`start`, `end`)
    """
    return tstamp >= start and tstamp < end


def parse_string_to_timestamp(tstamp: str) -> datetime:
    """
    Parses a string from Qualtrics into a datetime using the standard Qualtrics timestamp datetime format.
    """
    qualtrics_timestamp_format = "%Y-%m-%dT%H:%M:%S%z"
    return datetime.strptime(tstamp, qualtrics_timestamp_format)


def get_and_write_surveys(qualtrics_client: QualtricsClient) -> List[str]:
    """
    Retrieves all surveys from Qualtrics and writes their json out to `surveys.json`.
    Returns a list of all of the survey ids.
    """
    surveys_to_write = [survey for survey in qualtrics_client.get_surveys()]
    if surveys_to_write:
        with open("surveys.json", "w") as out_file:
            json.dump(surveys_to_write, out_file)
        snowflake_stage_load_copy_remove(
            "surveys.json",
            "raw.qualtrics.qualtrics_load",
            "raw.qualtrics.survey",
            snowflake_engine,
        )
    return [survey["id"] for survey in surveys_to_write]


def get_distributions(qualtrics_client: QualtricsClient, survey_id: str) -> List[Dict[Any, Any]]:
    """
    Gets all distributions for the given survey id.
    Returns the entire distribution object.
    """
    return [
        distribution for distribution in qualtrics_client.get_distributions(survey_id)
    ]


def chunk_list(
    list_to_chunk: List[Any], chunk_size: int
) -> Generator[List[Any], None, None]:
    """A generator that chunks the given list into lists of length `chunk_size` or less"""
    for i in range(0, len(list_to_chunk), chunk_size):
        yield list_to_chunk[i : i + chunk_size]

def get_and_write_distributions(survey_ids: List[str]) -> List[Dict[Any, Any]]:
    """Gets all distributions for the given surveys and writes them to Snowflake as well as returns them."""
    all_distributions: List[Dict[Any, Any]] = []
    for survey_id in surveys_ids:
        current_distributions = get_distributions(client, survey_id)
        all_distributions = all_distributions + current_distributions
        if current_distributions:
    
            for chunk in chunk_list(current_distributions, 100):
                with open("distributions.json", "w") as out_file:
                    json.dump(chunk, out_file)
    
                snowflake_stage_load_copy_remove(
                    "distributions.json",
                    "raw.qualtrics.qualtrics_load",
                    "raw.qualtrics.distribution",
                    snowflake_engine,
                )
    return all_distributions


def get_and_write_contacts(distributions: List[Dict[Any, Any]]) -> List[Dict[Any, Any]]:
    """Gets all contacts associated with the mailing lists of the given distributions and writes them to Snowflake as well as returns them."""
    contacts_to_write = []
    for distribution in distributions_with_mailings_to_write:
        mailing_list_id = distribution["recipients"]["mailingListId"]
        if mailing_list_id:
            for contact in client.get_contacts(POOL_ID, mailing_list_id):
                contact["mailingListId"] = mailing_list_id
                contacts_to_write.append(contact)
    if len(contacts_to_write) > 0:
        with open("contacts.json", "w") as out_file:
            json.dump(contacts_to_write, out_file)

        snowflake_stage_load_copy_remove(
            "contacts.json",
            "raw.qualtrics.qualtrics_load",
            "raw.qualtrics.contact",
            snowflake_engine,
        )
    return contacts_to_write

if __name__ == "__main__":
    config_dict = env.copy()
    client = QualtricsClient(
        config_dict["QUALTRICS_API_TOKEN"], config_dict["QUALTRICS_DATA_CENTER"]
    )
    start_time = parse_string_to_timestamp(config_dict["START_TIME"])
    end_time = parse_string_to_timestamp(config_dict["END_TIME"])
    POOL_ID = config_dict["QUALTRICS_POOL_ID"]
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    surveys_to_write: List[str] = get_and_write_surveys(client)

    all_distributions = get_and_write_distributions(surveys_to_write)

    distributions_with_mailings_to_write = [
        distribution
        for distribution in all_distributions
        if timestamp_in_interval(
            parse_string_to_timestamp(distribution["sendDate"]), start_time, end_time
        )
    ]

    get_and_write_contacts(distributions_with_mailings_to_write) #return value not used right now

