#!/usr/bin/env python3
import os
from os.path import dirname

import re
import json
import yaml


# First run dbt list with the table names / lineage to look for and export it to the root directory for the dbt project and name the file `to_check.txt` example:

# dbt list -m sfdc_account_source+ netsuite_transaction_lines_source+ --exclude date_details dim_date > to_check.txt

# Assumes the Periscope and the Sisense-Safe directory was checked out at the parent repository

dirname = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
parentdirname = os.path.dirname(dirname)


def get_file_display_name(filepath: str) -> str:
    """
    Gets the dispay name from yaml file based on file path
    """

    if os.path.exists(filepath):
        with open(filepath) as f_:
            dataMap = yaml.safe_load(f_)
            display_name = dataMap["display_name"]
    else:
        display_name = "Untitled"

    return display_name


def get_dashboard_name(sql_path: str) -> str:
    """
    Gets the file path and name for the dashboard meta data.
    """

    yaml_root = os.path.dirname(sql_path)
    dashboard_dir_list = yaml_root.rsplit("/", 1)
    sql_file_name = dashboard_dir_list[-1].split(".")[0]
    yaml_path = f"{yaml_root}/{sql_file_name}.yaml"
    dashboard_name = get_file_display_name(yaml_path)

    return dashboard_name


def get_chart_name(sql_path: str) -> str:
    """
    Gets the file path and name for the chart meta data.
    """

    yaml_root = os.path.dirname(sql_path)
    sql_file_name = sql_path.split("/")[-1]
    yaml_file_name = sql_file_name.split(".")[0]
    yaml_path = f"{yaml_root}/{yaml_file_name}.yaml"
    chart_name = get_file_display_name(yaml_path)

    return chart_name


# Get the file path for all files with a line that matches a table, view, or snippet call.
paths_to_check = ["snippets", "views", "dashboards"]

repos_to_check = ["periscope", "sisense-safe"]

periscope_table_dict = dict()
for repo in repos_to_check:
    for path in paths_to_check:
        fullpath = f"{parentdirname}/{repo}/{path}"
        for root, dirs, files in os.walk(fullpath):
            for file in files:
                if file.endswith(".sql"):
                    full_filename = f"{root}/{file}"
                    # print(full_filename)
                    with open(full_filename, "r") as f:
                        lines = f.readlines()
                        all_lines = " ".join(lines)
                        # Removes new lines following "from" and "join" b/c people don't follow style guide
                        clean_lines = re.sub(
                            r"(from|join)([\r\n]*)", r"\1 ", all_lines.lower()
                        )
                        new_lines = clean_lines.split("\n")

                        for line in new_lines:
                            # Attempts to get the direct table calls as well as references to views and snippets.
                            # It will catch more than is needed but the unneeded matched tend to not get matched in latter steps.
                            matches = re.search(
                                r"(?:(?=\[)| (?:legacy|common(?:.*?)|workspace(?:.+?)|boneyard|restricted_safe(?:.+?))\.)([\_A-z0-9\(\'\[\]\'\)]*)",
                                line.lower(),
                            )
                            if matches is not None:
                                for match in matches.groups():
                                    # Strip prefixes
                                    simplified_name = re.sub(
                                        ".*\/analytics\/periscope\/", "", full_filename
                                    )
                                    # print(match)
                                    clean_match = re.sub("[\(].*?[\)]", "", match)
                                    match_table = periscope_table_dict.get(
                                        clean_match, {}
                                    )

                                    # The repo name could be an other level in the dictionary but it seemed to add un needed complexity to subsequent code.
                                    label = f"{repo}-{path}"

                                    # Dashboards have an additional layer of structure added so that the chart names can be included as well as the dashboard names.
                                    if path == "dashboards":
                                        dashboard_name = get_dashboard_name(root)
                                        chart_name = get_chart_name(simplified_name)
                                        # print(chart_name)
                                        match_dashboard = match_table.get(label, {})
                                        match_dashboard.setdefault(
                                            dashboard_name, set()
                                        ).add(
                                            chart_name
                                        )  #  file.lower()
                                        match_table[label] = match_dashboard
                                    else:
                                        match_table.setdefault(label, set()).add(
                                            file.lower()
                                        )

                                    periscope_table_dict[clean_match] = match_table


# Load in the list of models to match
models_to_match = set()
models_filename = f"{dirname}/transform/snowflake-dbt/to_check.txt"
with open(models_filename, "r") as f:
    lines = f.readlines()
    for line in lines:
        models_to_match.add(line.strip().lower().split(".")[-1])


# Recursively get all views and snippets that match and the views and sippets that use them.
to_match = set()
to_add = models_to_match.copy()
i = 1
while len(to_add) > 0 and i < 7:  # A catch for run away loops
    to_check = to_add.copy()
    to_add = set()
    for line in to_check:
        match = periscope_table_dict.get(line, dict())
        to_match = to_match.union(
            set().union(
                *[
                    value
                    for key, value in match.items()
                    if key not in {"sisense-safe-dashboards", "periscope-dashboards"}
                ]
            )
        )
    for value in to_match:
        table = f"[{value.lower().split('.')[0]}]"
        if table not in to_check and table not in models_to_match:
            to_add.add(table)

    models_to_match = models_to_match.union(to_add)
    i = i + 1

# Restructure the lowest sets to arrays for json style formating
output_dict = dict()
for line in models_to_match:
    match = periscope_table_dict.get(line, [])

    if len(match) > 0:

        for key, value in match.items():
            if key in {"sisense-safe-dashboards", "periscope-dashboards"}:
                charts = match[key]
                for keys, values in charts.items():
                    charts[keys] = list(values)
            else:
                match[key] = list(value)

        output_dict[line.strip()] = match


with open(f"{dirname}/transform/snowflake-dbt/sisense_elements.json", "w") as fp:
    json.dump(output_dict, fp)
