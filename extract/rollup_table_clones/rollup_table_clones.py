from gitlabdata.orchestration_utils import (
    data_science_engine_factory,
    query_dataframe,
   # snowflake_stage_load_copy_remove,
)
from googleapiclient.discovery import build
from yaml import load, safe_load, YAMLError
from os import environ as env
import pandas as pd
from io import BytesIO
from apiclient.http import MediaIoBaseDownload
from sqlalchemy.engine.base import Engine
import numpy as np
import time
import logging
import sys
import os


def get_table_column_names(engine, table_name):
    # TODO: Correct table reference.
    query = f"SELECT " \
            f"ordinal_position," \
            f"table_name," \
            f"column_name," \
            f"data_type," \
            f"character_maximum_length," \
            f"column_name || data_type as compare_column " \
            f"FROM RAW.INFORMATION_SCHEMA.COLUMNS " \
            f"WHERE TABLE_NAME = '{table_name}' order by 1"
    return query_dataframe(engine, query)


def process_merged_row(row):
    existing_data_type = row['data_type_y']
    joined_row = row['data_type_x']
    if type(joined_row) == float:
        return

    if type(existing_data_type) == str:
        return f"{row['column_name']}::{row['data_type_y']} AS {row['column_name']} ,"
    else:
        return f"{row['column_name']}::{row['data_type_x']} AS {row['column_name']} ,"

def get_tables_to_roll_up(engine, table_name):
    schema_check = f"SELECT table_name " \
                   f"FROM RAW.INFORMATION_SCHEMA.TABLES" \
                   f"WHERE RIGHT(TABLE_NAME, 2) = '08'" \
                   f"AND LEFT(TABLE_NAME, {len(table_name)}) = '{table_name}' " \
                   f"ORDER BY 1"
    return  query_dataframe(engine, schema_check)["table_name"]


def rollup_table_clone(engine, table_name):
    roll_up_table_info = get_table_column_names(engine, f"{table_name}_ROLLUP")
    tables_to_roll_up = get_tables_to_roll_up(engine, table_name)
    for items in tables_to_roll_up.iteritems():
        print(f"Processing {items[1]}")
        column_info = get_table_column_names(engine, items[1])

        column_string = ", ".join((column_info)['column_name'].unique())
        # Probably break this down, bit ridiculous as a one liner.

        merged_df = pd.merge(column_info, roll_up_table_info, how="outer", on="column_name", )
        select_string = " "
        for i, row in merged_df.iterrows():
            processed_row = process_merged_row(row)
            if processed_row:
                select_string = select_string + processed_row

        insert_stmt = f"INSERT INTO {table_name}_ROLLUP ({column_string}, ORIGINAL_TABLE_NAME) SELECT {select_string} '{items[1]}' as ORIGINAL_TABLE_NAME FROM RAW.FULL_TABLE_CLONES.{items[1]}"

def process_row(row):
    character_len = row["character_maximum_length"]
    if character_len and character_len > 0 and row['data_type'] != "TEXT":
        return f"{row['column_name']} {row['data_type']} ({character_len}) ,"
    else:
        return f"{row['column_name']} {row['data_type']} ,"

def create_rollup_table(engine, table_name):
    tables_to_roll_up = get_tables_to_roll_up(engine, table_name)
    latest_table_name = max(tables_to_roll_up.iteritems())[1]
    rollup_table_name = f"{table_name}_ROLLUP"

    latest_table_columns = get_table_column_names(engine, latest_table_name)

    big_df = latest_table_columns

    for items in tables_to_roll_up.iteritems():
        print(f"Processing {items[1]}")
        other_data = get_table_column_names(engine, items[1])
        big_df = big_df.append(other_data[~other_data['compare_column'].isin(big_df['compare_column'])])

    big_df = big_df.groupby(['column_name']).max().reset_index()

    query_dataframe(engine, f"DROP TABLE IF EXISTS {rollup_table_name}")

    create_table_statement = f"CREATE TABLE {rollup_table_name} ("

    for i, row in big_df.iterrows():
        create_table_statement = create_table_statement + process_row(row)

    create_table_statement = create_table_statement[:-1] + ', ORIGINAL_TABLE_NAME TEXT)'

    query_dataframe(engine, create_table_statement)
