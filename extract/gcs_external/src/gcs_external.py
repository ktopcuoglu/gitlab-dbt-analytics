import logging
import sys
from os import environ as env

from fire import Fire
from gitlabdata.orchestration_utils import snowflake_engine_factory
from sqlalchemy.engine import Engine


def get_load_command(stage_path):
    """
    Generate a load command based on date
    """
    try:
        logging.info("Generating load command...")

        load_command = f"""
          select 'dt={execution_date}'
        """

    except:
        logging.info("Failed to get copy command...")
    finally:
        return load_command


def load_data(model, sensitive, timestamp, inc_start, inc_end):
    """
    run copy command to copy data from snowflake
    """
    logging.info("Preparing copy data...")
    config_dict = env.copy()
    engine = snowflake_engine_factory(config_dict, "SYSADMIN")
    logging.info(f"Engine Created: {engine}")

    try:
        connection = engine.connect()
        load_command = get_load_command(execution_date)
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
