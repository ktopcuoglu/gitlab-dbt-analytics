"""
Module is used to transform SQL syntax from Postgres to Snowflake style
"""
import json
from typing import Any, Dict, List
from logging import info

from os import environ as env
from flatten_dict import flatten
from flatten_dict.reducer import make_reducer
import sqlparse
from sqlparse.sql import Token, TokenList
from sqlparse.tokens import Whitespace

import requests


def get_sql_query_map(private_token: str = None) -> Dict[Any, Any]:
    """
    Routine to get data from RestFUL API and return as a dict
    """
    headers = {
        "PRIVATE-TOKEN": private_token,
    }

    response = requests.get(
        "https://gitlab.com/api/v4/usage_data/queries", headers=headers
    )
    source_json_data = json.loads(response.text)

    return source_json_data


def optimize_token_size(input_token: str) -> str:
    """
    Function reduce and optimize the size of the token length
    Primary goal is to:
    - reduce multiple whitespaces:
      from ' ',' ',' ' to ' ' and decrease the size
      (which will speed up the process)
    - remove '\n' (new line) characters as
      they have no any value for a query execution
    """
    if not input_token:
        return ""

    optimized_token = []
    current_pos = 0
    previous_input = " "

    for current_input in input_token:
        if (
            current_input != previous_input or previous_input != " "
        ) and current_input != "\n":
            optimized_token.append(input_token[current_pos])
        previous_input = current_input
        current_pos += 1

    # Remove empty spaces from the end
    for reverse_optimized in optimized_token[::-1]:
        if reverse_optimized == " ":
            optimized_token.pop()
        else:
            break
    return "".join(optimized_token)


def translate_postgres_snowflake_count(input_token_list: list) -> List[str]:
    """
    Function to support translation of COUNT syntax from Postgres to Snowflake.
    Example:
         - translate count(xx.xx.xx) -> COUNT(xx.xx)
         - translate COUNT(DISTINCT xx.xx.xx) -> COUNT(DISTINCT xx.xx)
    """
    if not input_token_list:
        return []

    for index, token in enumerate(input_token_list):
        token = str(token).replace("COUNT( ", "COUNT(")
        check_postgres_count = str(token).upper()
        if (
            check_postgres_count.startswith("COUNT")
            and check_postgres_count.endswith(")")
            and check_postgres_count.count("(") > 0
            and check_postgres_count.count("(") == check_postgres_count.count(")")
            and check_postgres_count.count(".") == 2
        ):
            # distinguish COUNT(DISTINCT xx.xx.xx) from COUNT(xx.xx.xx)
            if " " in token[6:]:
                index_from = check_postgres_count.index(" ")
            else:
                index_from = check_postgres_count.index("(")
            fixed_count = (
                str(token)[: index_from + 1] + str(token)[token.index(".") + 1 :]
            )

            input_token_list[index] = fixed_count
    return input_token_list


def prepare_sql_statement(input_token_list: list) -> str:
    """
    Transform token list in a prepared, executable SQL statement
    ready for the execution in Snowflake.
    """
    if not input_token_list:
        return ""

    # transform token list in list of strings
    prepared_list = [str(token) for token in input_token_list]

    # recreate from the list the SQL query
    prepared_query = "".join(prepared_list)

    return prepared_query


def find_keyword_index(input_token_list: list, defined_keyword: str) -> int:
    """
    Find the index, if any exist for keyword defined
    If no index is found, return 0 as a value
    """
    if not input_token_list:
        return 0

    for index, keyword_token in enumerate(input_token_list):
        if (
            keyword_token.is_keyword
            and str(keyword_token).upper() == defined_keyword.upper()
        ):
            return index
    return 0


def sql_queries_dict(input_json_data: Dict[Any, Any]) -> Dict[Any, Any]:
    """
    function that transforms the given json file into a Python dict with only SQL batch counters
    """

    full_payload_dict = flatten(input_json_data, reducer=make_reducer(delimiter="."))

    sql_dict = {}

    for key, value in full_payload_dict.items():
        # Check if key is even then add pair to new dictionary
        if isinstance(value, str) and value.startswith("SELECT"):
            sql_dict[key] = value

    return sql_dict


def add_counter_name_as_column(sql_metrics_name: str, sql_query: str) -> str:
    """
    Transform query from Postgres to Snowflake syntax.
    For this purpose using the sqlparse library: https://pypi.org/project/sqlparse/

    Example:
    A query like:

        SELECT COUNT(issues.id)
        FROM issues

    will be changed to:

        SELECT
          "counts.issues" AS counter_name,
          COUNT(issues.id) AS counter_value,
          TO_DATE(CURRENT_DATE) AS run_day
        FROM issues
    """

    # removing extra " to have an easier query to parse
    sql_query = sql_query.replace('"', "")
    sql_query_optimized = optimize_token_size(input_token=sql_query)

    sql_query_parsed = sqlparse.parse(sql_query_optimized)

    # split the query in tokens
    # get a list of tokens
    token_list = sql_query_parsed[0].tokens

    # optimize token_list size
    select_index = find_keyword_index(
        input_token_list=token_list, defined_keyword="SELECT"
    )
    from_index = find_keyword_index(input_token_list=token_list, defined_keyword="FROM")

    token_list_with_counter_name = translate_postgres_snowflake_count(
        input_token_list=token_list
    )

    """
    Determinate if we have subquery (without FROM clause in the main query),
    in the format SELECT (SELECT xx FROM);
    If yes, fit it as a scalar value in counter_value column
    """
    if from_index == 0:
        from_index = len(token_list_with_counter_name) + 1

    # add the counter name column
    token_list_with_counter_name.insert(
        from_index - 1, " AS counter_value, TO_DATE(CURRENT_DATE) AS run_day  "
    )

    token_list_with_counter_name.insert(
        # select_index + 1, f" '{sql_metrics_name}' AS counter_name, "
        select_index + 1,
        " '" + sql_metrics_name + "' AS counter_name, ",
    )

    return prepare_sql_statement(input_token_list=token_list_with_counter_name)


def rename_table_name(
    keywords_to_look_at: List[str],
    token: Token,
    tokens: List[Token],
    index: int,
    token_string_list: List[str],
) -> None:
    """
    Replaces the table name in the query --
    represented as the list of tokens -- to make it able to run in Snowflake

    Does this by prepending `prep.gitlab_dotcom.gitlab_dotcom_`
    to the table name in the query and then appending `_dedupe_source`
    """

    if any(token_word in keywords_to_look_at for token_word in str(token).split(" ")):
        i = 1

        # Whitespaces are considered as tokens and should be skipped
        while tokens[index + i].ttype is Whitespace:
            i += 1

        next_token = tokens[index + i]
        if not str(next_token).startswith("prep") and not str(next_token).startswith(
            "prod"
        ):
            # insert, token list to string list, create the SQL query, reparse it
            # there is FOR sure a better way to do that
            token_string_list[index + i] = (
                f"prep.gitlab_dotcom.gitlab_dotcom_"
                f"{str(next_token)}"
                f"_dedupe_source AS "
                f"{str(next_token)}"
            )


def rename_query_tables(sql_query: str) -> str:
    """
    function to rename the table in the sql query
    """

    # comprehensive list of all the keywords that are followed by a table name
    keywords_to_look_at = [
        "FROM",
        "JOIN",
    ]

    # start parsing the query and get the token_list
    parsed = sqlparse.parse(sql_query)
    tokens = list(TokenList(parsed[0].tokens).flatten())
    token_string_list = list(map(str, tokens))

    # go through the tokens to find the tables that should be renamed
    # for index in range(len(tokens)):
    for index, token in enumerate(tokens, start=0):
        rename_table_name(
            keywords_to_look_at=keywords_to_look_at,
            token=token,
            tokens=tokens,
            index=index,
            token_string_list=token_string_list,
        )
    return "".join(token_string_list)


def main(json_query_list: Dict[Any, Any]) -> Dict[Any, Any]:
    """
    Main input point to transform queries
    """

    sql_queries_dictionary = sql_queries_dict(json_query_list)

    sql_queries_dict_with_new_column = {
        metric_name: add_counter_name_as_column(
            sql_metrics_name=metric_name, sql_query=sql_queries_dictionary[metric_name]
        )
        for metric_name in sql_queries_dictionary
    }

    transformed_sql_query_dict = {
        metric_name: rename_query_tables(
            sql_query=sql_queries_dict_with_new_column[metric_name]
        )
        for metric_name in sql_queries_dict_with_new_column
    }

    return transformed_sql_query_dict


if __name__ == "__main__":
    config_dict = env.copy()
    json_data = get_sql_query_map(
        private_token=config_dict["GITLAB_ANALYTICS_PRIVATE_TOKEN"]
    )

    info("Processed sql queries")
    final_sql_query_dict = main(json_query_list=json_data)
    info("Processed final sql queries")

    with open(
        file="transformed_instance_queries.json", mode="w", encoding="utf-8"
    ) as f:
        json.dump(final_sql_query_dict, f)
