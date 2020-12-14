import subprocess
from os import environ as env


def get_create_statement_with_varchars(table_name, column_names):
    column_list = ",".join([f"{column_name} VARCHAR " for column_name in column_names])
    return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {column_list}
        );
    """


def parse_sql_query_to_columns(query):
    select_clause = query.split("FROM")[0]
    select_clause_without_select = select_clause.split("SELECT", 1)[1].strip()
    column_list_not_clean = select_clause_without_select.split(",")
    column_list = [column.strip() for column in column_list_not_clean]
    return column_list


def get_query_env_variable(copy_env):
    return copy_env["query"]


print(
    get_create_statement_with_varchars(
        env["table_name"], parse_sql_query_to_columns(get_query_env_variable(env))
    )
)
