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
        
        create or replace temporary table "RAW"."CONTAINER_REGISTRY"."joined_downloaded_tmp" as (

          with blob_downloaded as (
          
          select
            $1:correlation_id     as correlation_id,
            $1:time::timestamp    as timestamp,
            $1:root_repo::varchar as root_repo,
            $1:vars_name::varchar as vars_name,
            $1:digest::varchar    as digest,
            $1:size_bytes::int    as size_bytes
          from @raw.container_registry.container_registry/dt={path_date}
            (file_format=>json_generic)
          where $1:msg='blob downloaded'
          
          ), access as (
          
          select
            $1:correlation_id     as correlation_id,
            $1:time::timestamp    as timestamp,
            $1:remote_ip::varchar as remote_ip
          from @raw.container_registry.container_registry/dt={path_date}
            (file_format=>json_generic)
          where $1:msg='access'
          )
          
          select
            blob_downloaded.correlation_id,
            blob_downloaded.timestamp,
            blob_downloaded.root_repo,
            blob_downloaded.vars_name,
            blob_downloaded.digest,
            blob_downloaded.size_bytes,
            access.remote_ip
          from access
          inner join blob_downloaded on blob_downloaded.correlation_id = access.correlation_id
       
        );
    """

def load_data(execution_date):
    """
    run copy command to copy data from snowflake
    """
    logging.info("Preparing to load data...")
    config_dict = env.copy()
    path_date = config_dict["PATH_DATE"]
    engine = snowflake_engine_factory(config_dict, "LOADER")
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
