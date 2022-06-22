import logging
import sys
from os import environ as env

from fire import Fire
from gitlabdata.orchestration_utils import snowflake_engine_factory
from sqlalchemy.engine import Engine


def get_load_command(path_date: str) -> str:
    """
    Generate a load command based on date
    """
    return f"""
        SELECT '{start_time}', 'pizza'
    """

def load_data(execution_date):
    """
    run copy command to copy data from snowflake
    """
    logging.info("Preparing to load data...")
    config_dict = env.copy()
    path_date = "config_dict["PATH_DATE"]"
    engine = snowflake_engine_factory(config_dict, "SYSADMIN")
    logging.info(f"Engine Created: {engine}")

    try:
        connection = engine.connect()
        load_command = get_load_command(path_date)
        logging.info("running copy command {load_command}")
        connection.execute(load_command).fetchone()
    except:
        logging.info("Failed to run copy command...")
        raise
    finally:
        connection.close()
        engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=20)
    Fire(load_data)
    logging.info("Complete.")
