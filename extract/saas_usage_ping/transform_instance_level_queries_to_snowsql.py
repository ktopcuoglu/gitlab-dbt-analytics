import json
import re
import os

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
from sqlparse.utils import imt


## Workflow
### Read and transform JSON file


def sql_queries_dict(json_file):
    """
    function that transforms the sql-export.json file into a Python dict with only SQL batch counters
    """
    with open(json_file) as f:
        data = json.load(f)

    full_payload_dict = flatten(data, reducer=make_reducer(delimiter="."))

    sql_queries_dict = {}

    for key, value in full_payload_dict.items():
        # Check if key is even then add pair to new dictionary
        if isinstance(value, str) and value.startswith("SELECT"):
            sql_queries_dict[key] = value

    return sql_queries_dict


def add_counter_name_as_column(sql_metrics_name, sql_query):
    """
    step needed to add the first 2 columns:
      - counter_name
      - run_day

    this needs a specific row of a specific dataframe, I think this could be changed to a SQL query for more convenience

    a query like that SELECT COUNT(issues.id) FROM issues will be changed to SELECT "counts.issues", COUNT(issues.id), TO_DATE(CURRENT_DATE)

    needed for version 1 and 2
    """

    # removing extra " to have an easier query to parse
    sql_query = sql_query.replace('"', "")

    # using here the sqlparse library: https://www.google.com/search?q=sqlparse&oq=sqlparse&aqs=chrome..69i57j0l9.884j0j7&sourceid=chrome&ie=UTF-8
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
    token_list_with_counter_name = token_list[:]

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


def rename_table_name(keywords_to_look_at, token, tokens, index, token_string_list):
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
            # there is FOR sure a beter way to do that
            token_string_list[index + i] = (
                "prep.gitlab_dotcom.gitlab_dotcom_"
                + str(next_token)
                + "_dedupe_source AS "
            )
            token_string_list.insert(index + i + 1, str(next_token))


def rename_query_tables(sql_query):
    """
    function to rename the table based on a new regex
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
    # I find this for loop very confusing... there might be better ways to do it for sure
    for index in range(len(tokens)):
        token = tokens[index]
        rename_table_name(keywords_to_look_at, token, tokens, index, token_string_list)

    return "".join(token_string_list)


if __name__ == "__main__":
    ## Files imported

    ## counter queries (sql+redis): this is generated manually by the product intelligence team
    ## available here

    json_file_path = "./usage_ping_instance_queries.json"

    sql_queries_dictionary = sql_queries_dict(json_file_path)

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

    with open("all_sql_queries.draft.json", "w") as f:
        json.dump(final_sql_query_dict, f)
