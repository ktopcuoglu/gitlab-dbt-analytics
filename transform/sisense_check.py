#!/usr/bin/env python3

"""
Script to detect Views, Snippets and Dashboards that reference a dbt model. It also checks recursevely for snippets/views that use a snippet/view that reference
the dbt model.

The output is a `sisense_elements.json` file.

First run dbt list with the table names / lineage to look for and export it to the root directory for the dbt project and name the file `to_check.txt` example:

dbt list -m sfdc_account_source+ netsuite_transaction_lines_source+ --exclude date_details dim_date > to_check.txt

Assumes the Periscope Sisense Intermediate  Safe and the Sisense-Safe directory was checked out at the parent repository
"""


import os
import re
import json
import yaml


dirname = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
parentdirname = os.path.dirname(dirname)


def get_file_display_name(filepath: str) -> str:
    """
    Gets the dispay name from yaml file based on file path
    """

    if os.path.exists(filepath):
        with open(filepath, encoding="UTF-8") as file_to_load:
            data_map = yaml.safe_load(file_to_load)
            display_name = data_map["display_name"]
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
    dashboard_display_name = get_file_display_name(yaml_path)

    return dashboard_display_name


def get_chart_name(sql_path: str) -> str:
    """
    Gets the file path and name for the chart meta data.
    """

    yaml_root = os.path.dirname(sql_path)
    sql_file_name = sql_path.split("/")[-1]
    yaml_file_name = sql_file_name.split(".")[0]
    yaml_path = f"{yaml_root}/{yaml_file_name}.yaml"
    chart_display_name = get_file_display_name(yaml_path)

    return chart_display_name


# Get the file path for all files with a line that matches a table, view, or snippet call.
paths_to_check = ["snippets", "views", "dashboards"]

repos_to_check = ["periscope", "sisense-safe", "sisense-safe-intermediate"]

periscope_table_dict = {}
for repo in repos_to_check:
    for path in paths_to_check:
        fullpath = f"{parentdirname}/{repo}/{path}"
        for root, dirs, files in os.walk(fullpath):
            for file in files:
                if file.endswith(".sql"):
                    full_filename = f"{root}/{file}"
                    # print(full_filename)
                    with open(full_filename, "r", encoding="UTF-8") as file_to_open_yml:
                        lines = file_to_open_yml.readlines()
                        ALL_LINES = " ".join(lines)
                        # Removes new lines following "from" and "join" b/c people don't follow style guide
                        clean_lines = re.sub(
                            r"(from|join)([\r\n]*)", r"\1 ", ALL_LINES.lower()
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
                                        r".*\/analytics\/periscope\/", "", full_filename
                                    )
                                    # print(match)
                                    clean_match = re.sub(r"[\(].*?[\)]", "", match)
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
                                        ).add(chart_name)
                                        match_table[label] = match_dashboard
                                    else:
                                        match_table.setdefault(label, set()).add(
                                            file.lower()
                                        )

                                    periscope_table_dict[clean_match] = match_table


# Load in the list of models to match
models_to_match = set()
models_filename = f"{dirname}/transform/snowflake-dbt/to_check.txt"
with open(models_filename, "r", encoding="UTF-8") as fp1:
    lines = fp1.readlines()
    for line in lines:
        models_to_match.add(line.strip().lower().split(".")[-1])


dashboards_spaces_to_check = [repo + "-dashboards" for repo in repos_to_check]

# Recursively get all views and snippets that match and the views and sippets that use them.
to_match = set()
to_add = models_to_match.copy()
i = 1
while len(to_add) > 0 and i < 7:  # A catch for run away loops
    to_check = to_add.copy()
    to_add = set()
    for line in to_check:
        match = periscope_table_dict.get(line, {})
        to_match = to_match.union(
            set().union(
                *[
                    value
                    for key, value in match.items()
                    if key not in dashboards_spaces_to_check
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
output_dict = {}
for line in models_to_match:
    match = periscope_table_dict.get(line, [])

    if len(match) > 0:

        for key, value in match.items():
            if key in dashboards_spaces_to_check:
                charts = match[key]
                for keys, values in charts.items():
                    charts[keys] = list(values)
            else:
                match[key] = list(value)

        output_dict[line.strip()] = match


with open(
    f"{dirname}/transform/snowflake-dbt/sisense_elements.json", "w", encoding="UTF-8"
) as fp:
    json.dump(output_dict, fp)
