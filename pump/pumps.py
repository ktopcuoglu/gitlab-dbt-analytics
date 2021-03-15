import logging
import sys
from os import environ as env

from fire import Fire
# from gitlabdata.orchestration_utils import snowflake_engine_factory
# from sqlalchemy.engine import Engine


def get_copy_command(model, sensitive, timestamp, inc_start, inc_end):
    """
    Generate a copy command based on data passed from pumps.yml
    """
    try:
        logging.info("Getting copy command...")

        from_statement = "FROM PROD.{schema}.{model}".format(
            model=model, schema="pumps" if sensitive == False else "pumps_sensitive"
        )

        where_statement = (
            "WHERE {timestamp} between '{inc_start}' and '{inc_end}'".format(
                timestamp=timestamp,
                inc_start=inc_start,
                inc_end=inc_end,
            )
        )

        if timestamp == None:
            query = "SELECT * " + from_statement
        else:
            query = "SELECT * " + from_statement + where_statement

        
        copy_command_tmp = """
        COPY INTO @RAW.PUBLIC.S3_DATA_PUMP/{model}
        FROM ({query})
        FILE_FORMAT = (TYPE = CSV, NULL_IF = (), FIELD_OPTIONALLY_ENCLOSED_BY = '"', COMPRESSION=NONE)
        HEADER = TRUE
        INCLUDE_QUERY_ID = TRUE;
      """

        copy_command = copy_command_tmp.format(
            model=model,
            query=query,
        )
    
    except:
        logging.info("Failed to get copy command...")
    finally:
        logging.info({copy_command})
        return copy_command


def copy_data(model, sensitive, timestamp, inc_start, inc_end):
    """
    run copy command to copy data from snowflake
    """
    logging.info("Preparing copy data...")
    config_dict = env.copy()
    engine = snowflake_engine_factory(config_dict, "SYSADMIN")
    logging.info(f"Engine Created: {engine}")

    try:
        connection = engine.connect()
        copy_command = get_copy_command(model, sensitive, timestamp)
        logging.info("running copy command")
        connection.execute(copy_command)
    except:
        logging.info("Failed to run copy command...")
    finally:
        connection.close()
        engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=20)
    Fire(get_copy_command)
    logging.info("Complete.")
