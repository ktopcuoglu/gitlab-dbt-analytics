#!/usr/bin/env python3
import os
from os.path import join, getsize, dirname
import re
import json
import yaml


# First run dbt list with the table names / lineage to look for and export it to the root directory for the dbt project and name the file `to_check.txt` example:

# dbt list -m sfdc_account_source+ netsuite_transaction_lines_source+ --exclude date_details dim_date > to_check.txt

# Assumes the Periscope directory was checked out at the parent repository

dirname = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
parentdirname = os.path.dirname(dirname)

def get_dashboard_name (sql_path):
    yaml_root = os.path.dirname(sql_path)
    dashboard_dir_list = yaml_root.rsplit('/',1)
    sql_file_name = dashboard_dir_list[-1].split('.')[0]
    yaml_path = f"{yaml_root}/{sql_file_name}.yaml" # check for existence
    if os.path.exists(yaml_path):
        with open(yaml_path) as f_:
            dataMap = yaml.safe_load(f_)
            dashboard_name = dataMap['display_name']
    else:
       dashboard_name = 'Untitled' 

    return dashboard_name

paths_to_check = ["snippets","views","dashboards"]

periscope_table_dict = dict()

for path in paths_to_check:
    fullpath = f"{parentdirname}/periscope/{path}"  #TODO have it work for the SAFE space as well
    for root, dirs, files in os.walk(fullpath):
        for file in files:
            if file.endswith('.sql'):
                full_filename = f"{root}/{file}"
                #print(full_filename)
                with open(full_filename, "r") as f:
                    lines = f.readlines()
                    all_lines = " ".join(lines)
                    # Removes new lines following "from" and "join" b/c people don't follow style guide
                    clean_lines = re.sub(
                        r"(from|join)([\s\\r\\n]*)", r"\1 ", all_lines.lower()
                    )
                    new_lines = clean_lines.split("\n")

                    for line in new_lines:

                        matches = re.search(
                            r"(?:(?=\[)| (?:legacy|common|common_mapping|boneyard)\.)([\_A-z0-9\(\'\[\]\'\)]*)",
                            line.lower(),
                        )
                        if matches is not None:
                            for match in matches.groups():
                                # Strip prefixes
                                simplified_name = re.sub(
                                    ".*\/analytics\/periscope\/", "", full_filename
                                )
                                clean_match = re.sub("[\(].*?[\)]", "", match)
                                match_table = periscope_table_dict.get(clean_match, {})

                                if path == 'dashboards':
                                    dashboard_name = get_dashboard_name(root)
                                    #print(dashboard_name)
                                    match_dashboard = match_table.get(path,{})
                                    match_dashboard.setdefault(dashboard_name,set()).add(file.lower())
                                    match_table[path] = match_dashboard
                                else:
                                    match_table.setdefault(path, set()).add(file.lower())
                                
                                periscope_table_dict[clean_match] = match_table
                                
#print(periscope_table_dict)

models_to_match = set()
models_filename = f"{dirname}/transform/snowflake-dbt/to_check.txt"
with open(models_filename, "r") as f:
    lines = f.readlines()
    for line in lines:
        models_to_match.add(line.strip().lower().split('.')[-1])

to_match = set()
to_add =  models_to_match.copy() #{'[bamboohr_rpt_headcount_aggregation]', '[bamboohr_rpt_division_level]'} #
i = 1
while len(to_add) > 0 and i < 7:
    to_check = to_add.copy()
    to_add = set()
    for line in to_check:
        match = periscope_table_dict.get(line, dict())
        to_match = to_match.union(set().union(*[value for key, value in match.items() if key not in {'dashboards'}]))
    for value in to_match:
        table =  f"[{value.lower().split('.')[0]}]"
        if table not in to_check and table not in models_to_match:
            to_add.add(table)  
    
    models_to_match = models_to_match.union(to_add)
    i = i + 1


output_dict = dict()
for line in models_to_match:
    match = periscope_table_dict.get(line, [])

    if len(match) > 0:

        for key, value in match.items():
            if key == 'dashboards':
                charts = match[key]
                for keys, values in charts.items():
                    charts[keys] = list(values)
            else:
                match[key] = list(value)

        output_dict[line.strip()] = match

#print(output_dict)

with open(f"{dirname}/transform/snowflake-dbt/sisense_elements.json", 'w') as fp:
    json.dump(output_dict, fp)
