"""
The main module for gcs
"""
import logging
from os import environ as env

from fire import Fire
from gitlabdata.orchestration_utils import snowflake_engine_factory


def get_load_command(path_date: str) -> str:
    """
    Generate a load command based on date
    """
    return f"""

        CREATE OR REPLACE TABLE "CONTAINER_REGISTRY"."JOINED_{path_date.replace('-','_')}" AS (

          WITH blob_downloaded AS (
          SELECT
            $1:correlation_id::VARCHAR     AS correlation_id,
            $1:time::TIMESTAMP             AS timestamp,
            $1:root_repo::VARCHAR          AS root_repo,
            $1:vars_name::VARCHAR          AS vars_name,
            $1:digest::VARCHAR             AS digest,
            $1:size_bytes::INT             AS size_bytes
          FROM @container_registry.container_registry/dt={path_date}
            (file_format=>json_generic)
            WHERE $1:msg='blob downloaded'
            ), access AS (
          SELECT
            $1:correlation_id::VARCHAR     AS correlation_id,
            $1:time::TIMESTAMP             AS timestamp,
            $1:remote_ip::VARCHAR          AS remote_ip
          FROM @container_registry.container_registry/dt={path_date}
            (file_format=>json_generic)
          WHERE $1:msg='access'
          )
          SELECT
            blob_downloaded.correlation_id,
            blob_downloaded.timestamp,
            blob_downloaded.root_repo,
            blob_downloaded.vars_name,
            blob_downloaded.digest,
            blob_downloaded.size_bytes,
            access.remote_ip
          FROM access
          INNER JOIN blob_downloaded ON blob_downloaded.correlation_id = access.correlation_id

        );
    """


def load_data():
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
        logging.info(f"running copy command {load_command}")
        results = connection.execute(load_command).fetchone()
        logging.info(results)
    except:
        logging.error("Failed to load")
        raise
    finally:
        connection.close()
        engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=20)
    Fire(load_data)
    logging.info("Complete.")
