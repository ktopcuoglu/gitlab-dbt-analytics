import json
from typing import Any, Dict, List
from logging import error, info, basicConfig, getLogger, warning

from flatten_dict import flatten
from flatten_dict.reducer import make_reducer
import sqlparse
from sqlparse.sql import (
    Identifier,
    IdentifierList,
    remove_quotes,
    Token,
    TokenList,
    Where,
)
from sqlparse.tokens import Keyword, Name, Punctuation, String, Whitespace
from os import environ as env
import requests

## Workflow
### Read and transform JSON file


def get_sql_query_map(private_token=None) -> str:
    headers = {
        "PRIVATE-TOKEN": private_token,
    }

    response = requests.get(
        "https://gitlab.com/api/v4/usage_data/queries", headers=headers
    )
    json_data = json.loads(response.text)

    return json_data


def sql_queries_dict(json_data: str) -> Dict[Any, Any]:
    """
    function that transforms the given json file into a Python dict with only SQL batch counters
    """

    full_payload_dict = flatten(json_data, reducer=make_reducer(delimiter="."))

    sql_queries_dict = {}

    for key, value in full_payload_dict.items():
        # Check if key is even then add pair to new dictionary
        if isinstance(value, str) and value.startswith("SELECT"):
            sql_queries_dict[key] = value

    return sql_queries_dict


def add_counter_name_as_column(sql_metrics_name: str, sql_query: str) -> str:
    """
    A query like

    SELECT COUNT(issues.id)
    FROM issues

    will be changed to

    SELECT
      "counts.issues" AS counter_name,
      COUNT(issues.id) AS counter_value,
      TO_DATE(CURRENT_DATE) AS run_day
    FROM issues
    """

    # removing extra " to have an easier query to parse
    sql_query = sql_query.replace('"', "")

    # using here the sqlparse library: https://pypi.org/project/sqlparse/
    sql_query_parsed = sqlparse.parse(sql_query)

    # split the query in tokens
    # get a list of tokens
    token_list = sql_query_parsed[0].tokens

    select_index = 0
    for index, token in enumerate(token_list):

        # identify if it is a select statement
        if token.is_keyword and str(token) == "SELECT":
            # set the select_index
            select_index = index
            break

    from_index = 0
    for index, token in enumerate(token_list):
        if token.is_keyword and str(token) == "FROM":
            from_index = index
            break

    # TODO: rbacovic - translate count(xx.xx.xx) into COUNT(xx.xx) make this more readable
    # Use case fixed: translate COUNT(xx.xx.xx) into COUNT(xx.xx)
    for index, token in enumerate(token_list):
        token = str(token).replace("COUNT( ", "COUNT(")
        check_postgres_count = str(token).upper()
        if (
            check_postgres_count.startswith("COUNT")
            and check_postgres_count.endswith(")")
            and check_postgres_count.count("(") == 1
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

            token_list[index] = fixed_count

    token_list_with_counter_name = token_list[:]

    # TODO: rbacovic determinate if we have subquery (without FROM clause), make this a little bit more readable
    # Use case fixed: put scalar sub query properly as a column value
    # if yes, fit it as a scalar value in counter_value column
    if from_index == 0:
        from_index = len(token_list_with_counter_name) + 1

    # add a name for the count columns and add the date column

    # add the counter name column
    token_list_with_counter_name.insert(
        from_index - 1, " AS counter_value, TO_DATE(CURRENT_DATE) AS run_day  "
    )
    token_list_with_counter_name.insert(
        select_index + 1, " '" + sql_metrics_name + "' AS counter_name, "
    )

    # transform token list in list of strings
    enhanced_query_list = [str(token) for token in token_list_with_counter_name]

    # recreate from the list the SQL query
    enhanced_query = "".join(enhanced_query_list)

    return enhanced_query


def rename_table_name(
    keywords_to_look_at: List[str],
    token: Token,
    tokens: List[Token],
    index: int,
    token_string_list: List[str],
) -> None:
    """
    Replaces the table name in the query -- represented as the list of tokens -- to make it able to run in Snowflake

    Does this by prepending `prep.gitlab_dotcom.gitlab_dotcom_`
    to the table name in the query and then appending `_dedupe_source`.
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

            token_string_list[
                index + i
            ] = f"prep.gitlab_dotcom.gitlab_dotcom_{str(next_token)}_dedupe_source AS {str(next_token)}"


def rename_query_tables(sql_query: str) -> str:
    """
    function to rename the table in the sql query
    """

    ### comprehensive list of all the keywords that are followed by a table name
    keywords_to_look_at = [
        "FROM",
        "JOIN",
    ]

    # start parsing the query and get the token_list
    parsed = sqlparse.parse(sql_query)
    tokens = list(TokenList(parsed[0].tokens).flatten())

    token_string_list = list(map(str, tokens))

    # go through the tokens to find the tables that should be renamed
    for index in range(len(tokens)):
        token = tokens[index]
        rename_table_name(keywords_to_look_at, token, tokens, index, token_string_list)

    return "".join(token_string_list)


if __name__ == "__main__":
    ## Files imported

    ## counter queries (sql+redis): this is generated manually by the product intelligence team
    ## available here

    config_dict = env.copy()
    json_data = get_sql_query_map(
        private_token=config_dict["GITLAB_ANALYTICS_PRIVATE_TOKEN"]
    )

    sql_queries_dictionary = sql_queries_dict(json_data)
    info("Processed sql queries")
    sql_queries_dict_with_new_column = {
        metric_name: add_counter_name_as_column(
            metric_name, sql_queries_dictionary[metric_name]
        )
        for metric_name in sql_queries_dictionary
    }

    final_sql_query_dict = {
        metric_name: rename_query_tables(sql_queries_dict_with_new_column[metric_name])
        for metric_name in sql_queries_dict_with_new_column
    }
    info("Processed final sql queries")

    with open("transformed_instance_queries.json", "w") as f:
        json.dump(final_sql_query_dict, f)
