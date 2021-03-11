import logging
import sys
import fire

logging.basicConfig(stream=sys.stdout, level=20)


def get_copy_command(model, sensitive, timestamp, inc_start, inc_end):
    try:
        logging.info("Getting copy command...")

        from_statement = "FROM PROD.{schema}.{model}".format(
            model=model, schema="pumps" if sensitive == False else "pumps_sensitive"
        )

        where_statement = "WHERE {timestamp} between {inc_start} and {inc_end}".format(
            timestamp=timestamp,
            inc_start=inc_start,
            inc_end=inc_end,
        )

        if timestamp == None:
            query = "SELECT * " + from_statement
        else:
            query = "SELECT * " + from_statement + where_statement

        copy_command_tmp = """
        COPY INTO @RAW.PUBLIC.S3_DATA_PUMP/{model}
        FROM ({query})
        FILE_FORMAT = (TYPE = CSV, FIELD_OPTIONALLY_ENCLOSED_BY = '"', COMPRESSION=NONE)
        HEADER = TRUE
        INCLUDE_QUERY_ID = TRUE;
      """

        copy_command = copy_command_tmp.format(
            model=model,
            query=query,
        )

        return copy_command

    except:
        logging.info("Failed to get copy command...")
    finally:
        return copy_command


if __name__ == "__main__":
    logging.basicConfig(level=20)
    fire.Fire(get_copy_command)
    logging.info("Complete.")
