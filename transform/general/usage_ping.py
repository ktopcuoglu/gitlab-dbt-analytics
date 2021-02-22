import json 
import re
import os

import pandas as pd
import sqlparse
import sql_metadata

from flatten_dict import flatten
from pprint import pprint
from sqlparse.sql import Identifier, IdentifierList, remove_quotes, Token, TokenList, Where
from sqlparse.tokens import Keyword, Name, Punctuation, String, Whitespace
from sqlparse.utils import imt

## Files imported

## counter queries (sql+redis)
json_file_path = '/usr/local/analytics/transform/general/usage_ping_sql.json'


## foreign_key csv generated
# foreign_key_df = pd.read_csv('/Users/mathieupeychet/Documents/foreign_keys.csv')

## To join an event to a ultimate_namespace_id, we have 3 potential standard tables to join
table_to_join = ['projects', 'namespaces', 'groups']

# with open(json_file_path) as f:
#     data = json.load(f)

def sql_queries_dict(json_file):
    ''' 
    function that transforms the sql-export.json file into a Python dict with only SQL batch counters
    '''
    with open(json_file) as f:
        data = json.load(f)

    from flatten_dict.reducer import make_reducer
    full_payload_dict = flatten(data, reducer=make_reducer(delimiter='.'))

    sql_queries_dict  = {}

    for (key, value) in full_payload_dict.items():
       # Check if key is even then add pair to new dictionary
       if isinstance(value, str) and str.startswith(value, 'SELECT') is True:
           sql_queries_dict[key] = value
    
    return sql_queries_dict


def create_join_mapping_df(sql_queries_dict):
    '''
        The function returns a dataframe with the following columns
        - counter: name of the counter, which is the item key in the dictionary passed
        - sql_query: query run to calculate the counter, item value in the dictionary passed as argument
        - table_name: name of the table in the FROM statement of the sql_query
        - foreingn_table_name: 
        - foreign_column_name:
    '''
    final_join_mapping_df = pd.DataFrame()
    for key, value in sql_queries_dict.items():
        sql_value = sqlparse.parse(value)[0]
        ## get the table which is queried in the FROM statement
        queried_tables_list = sql_metadata.get_query_tables(value)
        value = value.replace('"', "")
        
        value= re.sub('{:start=>[0-9]{1,}, :finish=>[0-9]{1,}}', 'id', value)
        # if projects is just do the join on projects
        # if 'projects' in queried_tables_list:
            # potential_joins = foreign_key_df[(foreign_key_df['table_name'] == 'projects') & (foreign_key_df['foreign_table_name'] == 'namespaces')]
            # potential_joins = potential_joins.drop_duplicates()
            # table_to_append = potential_joins[potential_joins.table_name == 'projects']
            # table_to_append["counter"] = key
            # table_to_append["sql_query"] = value
            # final_join_mapping_df = final_join_mapping_df.append(table_to_append, ignore_index=True)   
        # else:
        for index, queried_table in enumerate(queried_tables_list):

        ## 
            potential_joins = foreign_key_df[(foreign_key_df['table_name'] == queried_table) & (foreign_key_df['foreign_table_name'].isin(table_to_join))]
            potential_joins = potential_joins.drop_duplicates()
            
            if potential_joins[potential_joins.foreign_table_name == 'projects'].empty is False:
                table_to_append = potential_joins[potential_joins.foreign_table_name == 'projects']
                table_to_append["counter"] = key
                table_to_append["sql_query"] = value
                final_join_mapping_df = final_join_mapping_df.append(table_to_append, ignore_index=True)
            elif potential_joins[potential_joins.foreign_table_name == 'groups'].empty is False:
                table_to_append = potential_joins[potential_joins.foreign_table_name == 'groups']
                table_to_append["counter"] = key
                table_to_append["sql_query"] = value
                final_join_mapping_df = final_join_mapping_df.append(table_to_append, ignore_index=True)
            elif potential_joins[potential_joins.foreign_table_name == 'namespaces'].empty is False:
                table_to_append = potential_joins[potential_joins.foreign_table_name == 'namespaces']
                table_to_append["counter"] = key
                table_to_append["sql_query"] = value
                final_join_mapping_df = final_join_mapping_df.append(table_to_append, ignore_index=True)
            if table_to_append.empty is False:
                break
                
        
    return final_join_mapping_df

query_dict = sql_queries_dict(json_file_path)

print(create_join_mapping_df(query_dict))
