from api import BizibleSnowFlakeExtractor
from os import environ as env
import logging
import sys
from fire import Fire
from typing import Dict, Any
import yaml


def manifest_reader(file_path: str) -> Dict[str, Dict]:
    """
    Read a yaml manifest file into a dictionary and return it.
    """

    with open(file_path, "r") as file:
        manifest_dict = yaml.load(file, Loader=yaml.FullLoader)

    return manifest_dict


def filter_manifest(manifest_list: list, load_only_table: str = None) -> None:
    # When load_only_table specified reduce manifest to keep only relevant table config
    if load_only_table and load_only_table in manifest_list:
        manifest_list = [m for m in manifest_list if m == load_only_table]
        return manifest_list


def main(file_path: str, load_only_table: str = None) -> None:
    config_dict = env.copy()
    extractor = BizibleSnowFlakeExtractor(config_dict)

    logging.info(f"Reading manifest at location: {file_path}")
    manifest_list = manifest_reader(file_path)
    # When load_only_table specified reduce manifest to keep only relevant table config
    manifest_list = filter_manifest(manifest_list, load_only_table)

    for table in manifest_list:
        logging.info(f"Processing Table: {table}")
        extractor.extract_latest_bizible_file(table)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("snowflake.connector.cursor").disabled = True
    logging.getLogger("snowflake.connector.connection").disabled = True
    Fire({"tap": main})
