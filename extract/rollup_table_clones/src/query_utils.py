from gitlabdata.orchestration_utils import (
    query_dataframe,
    query_executor,
)
import pandas as pd
from sqlalchemy.engine.base import Engine
import logging


def get_table_column_names(engine: Engine, db_name: str, table_name: str) -> pd.DataFrame:
    """

    :param engine:
    :param db_name:
    :param table_name:
    :return:
    """
    query = f"SELECT " \
            f"  ordinal_position," \
            f"  table_name," \
            f"  column_name," \
            f"  data_type," \
            f"  character_maximum_length," \
            f"  column_name || data_type as compare_column " \
            f"FROM {db_name}.INFORMATION_SCHEMA.COLUMNS " \
            f"WHERE TABLE_NAME = '{table_name}' order by 1 "
    logging.info(f"Running {query}")
    return query_dataframe(engine, query)


def get_tables_to_roll_up(engine: Engine, db_name: str, table_name: str) -> pd.DataFrame:
    """

    :param engine:
    :param db_name:
    :param table_name:
    :return:
    :rtype:
    """
    schema_check = f"SELECT table_name " \
                   f"FROM {db_name}.INFORMATION_SCHEMA.TABLES " \
                   f"WHERE RIGHT(TABLE_NAME, 2) = '08' " \
                   f"AND LEFT(TABLE_NAME, {len(table_name)}) = '{table_name}' " \
                   f"ORDER BY 1"
    logging.info(f"Running {schema_check}")
    return query_dataframe(engine, schema_check)["table_name"]


def process_merged_row(row: pd.Series) -> str:
    """

    :param row:
    :return:
    :rtype:
    :rtype: object
    """
    existing_data_type = row['data_type_y']
    joined_row = row['data_type_x']
    if type(joined_row) == float:
        return ""

    if type(existing_data_type) == str:
        return f"{row['column_name']}::{row['data_type_y']} AS {row['column_name']} ,"
    else:
        return f"{row['column_name']}::{row['data_type_x']} AS {row['column_name']} ,"


def process_row(row: pd.Series) -> str:
    """

    :param row:
    :return:
    :rtype: object
    """
    character_len = row["character_maximum_length"]
    if character_len and character_len > 0 and row['data_type'] != "TEXT":
        return f"{row['column_name']} {row['data_type']} ({character_len}) ,"
    else:
        return f"{row['column_name']} {row['data_type']} ,"


def rollup_table_clone(engine: Engine, db_name: str, schema_name: str, table_name: str) -> bool:
    """

    :param engine:
    :param db_name:
    :param schema_name:
    :param table_name:
    :rtype: object
    """
    roll_up_table_info = get_table_column_names(engine, db_name, f"{table_name}_ROLLUP")
    logging.info(roll_up_table_info.head())
    if roll_up_table_info is None:
        recreate_rollup_table(engine, db_name, schema_name, table_name)
        roll_up_table_info = get_table_column_names(engine, db_name, f"{table_name}_ROLLUP")
    tables_to_roll_up = get_tables_to_roll_up(engine, db_name, table_name)
    logging.info(tables_to_roll_up.head())
    for items in tables_to_roll_up.iteritems():
        logging.info(f"Processing {items[1]}")
        column_info = get_table_column_names(engine, db_name, items[1])

        column_string = ", ".join((column_info)['column_name'].unique())
        merged_df = pd.merge(column_info, roll_up_table_info, how="outer", on="column_name", )
        select_string = " "
        for i, row in merged_df.iterrows():
            processed_row = process_merged_row(row)
            if processed_row:
                select_string = select_string + processed_row

        insert_stmt = f" INSERT INTO {db_name}.{schema_name}.{table_name}_ROLLUP " \
                      f"({column_string}, ORIGINAL_TABLE_NAME) " \
                      f" SELECT {select_string} '{items[1]}' as ORIGINAL_TABLE_NAME " \
                      f" FROM {db_name}.{schema_name}.{items[1]}"
        logging.info("Running query")
        query_executor(engine, insert_stmt)
        logging.info("Successfully rolled up table clones")

    return True


def recreate_rollup_table(engine: Engine, db_name: str, schema_name: str, table_name: str) -> bool:
    """

    :param engine:
    :param db_name:
    :param schema_name:
    :param table_name:
    :type table_name:
    :rtype: object
    """
    tables_to_roll_up = get_tables_to_roll_up(engine, db_name, table_name)
    latest_table_name = max(tables_to_roll_up.iteritems())[1]
    rollup_table_name = f"{db_name}.{schema_name}.{table_name}_ROLLUP"

    latest_table_columns = get_table_column_names(engine, db_name, latest_table_name)

    big_df = latest_table_columns

    for items in tables_to_roll_up.iteritems():
        logging.info(f"Processing {items[1]}")
        other_data = get_table_column_names(engine, db_name, items[1])
        big_df = big_df.append(other_data[~other_data['compare_column'].isin(big_df['compare_column'])])

    big_df = big_df.groupby(['column_name']).max().reset_index()

    query_executor(engine, f"DROP TABLE IF EXISTS {rollup_table_name}")

    create_table_statement = f"CREATE TABLE {rollup_table_name} ("

    for i, row in big_df.iterrows():
        create_table_statement = create_table_statement + process_row(row)

    create_table_statement = create_table_statement[:-1] + ', ORIGINAL_TABLE_NAME TEXT)'

    query_executor(engine, create_table_statement)

    return True

