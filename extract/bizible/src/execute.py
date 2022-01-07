from api import BizibleSnowFlakeExtractor
from os import environ as env
import logging
import sys


if __name__ == "__main__":

    config_dict = env.copy()

    logging.basicConfig(stream=sys.stdout, level=20)

    extractor = BizibleSnowFlakeExtractor(config_dict)

    bizible_tables = extractor.get_latest_bizible_tables()
    bizible_queries = extractor.get_bizible_queries(bizible_tables)

    extractor.extract_latest_bizible_files(bizible_queries)
