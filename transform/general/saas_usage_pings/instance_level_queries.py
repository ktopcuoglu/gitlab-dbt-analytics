#!/usr/bin/env python
# coding: utf-8

# In[7]:


#imported libraries

import json 
import sqlparse
from sqlparse.sql import Identifier, IdentifierList, remove_quotes, Token, TokenList, Where
from sqlparse.tokens import Keyword, Name, Punctuation, String, Whitespace
from sqlparse.utils import imt
import pandas as pd
from flatten_dict import flatten
from pprint import pprint
import sql_metadata
import re
import os
import urllib.request as request


# ## Some variables


## Files imported

## counter queries (sql+redis): this is generated manually by the product intelligence team
## available here

json_file_path = '/Users/mathieupeychet/Downloads/usage_ping_sql_jan.json'

## To join an event to a ultimate_namespace_id, we have 3 potential standard tables to join
table_to_join = ['projects', 'namespaces', 'groups']


# ## Workflow
# 
# ### Read and transform JSON file

# In[11]:


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

sql_queries_dictionary = sql_queries_dict(json_file_path)
print(sql_queries_dictionary)


def add_counter_name_as_column(sql_metrics_name, sql_query):
    '''
    step needed to add the first 2 columns:
      - counter_name
      - run_day
    
    this needs a specific row of a specific dataframe, I think this could be changed to a SQL query for more convenience
    
    a query like that SELECT COUNT(issues.id) FROM issues will be changed to SELECT 'counts.issues', COUNT(issues.id), TO_DATE(CURRENT_DATE)
    
    needed for version 1 and 2
    '''
    sql_query = sql_query.replace('"', "")
    sql_query_parsed = sqlparse.parse(sql_query)
    
    ### split the query in tokens
    token_list = sql_query_parsed[0].tokens
    from_index = 0
    
    for index, token in enumerate(token_list):
        #Token.Keyword.DML
        ### identify if it is a select statement
        if (token.is_keyword and str(token) == 'SELECT') is True:
            ### set the select_index
            select_index = index
            break
    for index, token in enumerate(token_list):
        if (token.is_keyword and str(token) == 'FROM') is True:
            from_index = index
            break
    token_list_with_counter_name = token_list[:]
    token_list_with_counter_name.insert(from_index - 1, " AS counter_value, TO_DATE(CURRENT_DATE) AS run_day  ")
    token_list_with_counter_name.insert(select_index + 1, " '" + sql_metrics_name + "' AS counter_name, ")
    enhanced_query_list = [str(token) for token in token_list_with_counter_name]
    enhanced_query = ''.join(enhanced_query_list)
    
    return enhanced_query



sql_queries_dict_with_new_column = { metric_name:add_counter_name_as_column(metric_name, sql_queries_dictionary[metric_name]) for metric_name in sql_queries_dictionary}
sql_queries_dict_with_new_column


def rename_query_tables(sql_query):
    '''
    function to rename the table based on a new regex
    '''
    
    ### comprehensive list of all the keywords that are followed by a table name
    keyword_to_look_at = [            
                'FROM',
                "JOIN",
                "INNER JOIN",
                "FULL JOIN",
                "FULL OUTER JOIN",
                "LEFT JOIN",
                "RIGHT JOIN",
                "LEFT OUTER JOIN",
                "RIGHT OUTER JOIN",
    ]

    
    ### start parsing the query and get the token_list
    parsed = sqlparse.parse(sql_query)
    tokens = list(TokenList(parsed[0].tokens).flatten())
    ### setting up to -1 to start
    keyword_token_index = -1
    
    while keyword_token_index != 0:
        keyword_token_index = 0
        
        ### go through the tokens
        for index, token in enumerate(tokens):
            if str(token) in keyword_to_look_at:
                keyword_token_index = index
                i = 1
                while tokens[index + i].ttype is Whitespace:
                    i += 1
                next_token = tokens[index + i]
                if str(next_token).startswith('prep') is False and str(next_token).startswith('prod') is False:
                    tokens.insert(keyword_token_index + i, "prep.gitlab_dotcom.gitlab_dotcom_" + str(next_token) + "_dedupe_source AS " )
                    tokens = [str(token) for token in tokens]
                    token_query = ''.join(tokens)
                    parsed = sqlparse.parse(token_query)
                    tokens = list(TokenList(parsed[0].tokens).flatten())
                    break
                else:
                    keyword_token_index = 0
            if keyword_token_index > 0:
                break
    return token_query
        

final_sql_query_dict = {metric_name: rename_query_tables(sql_queries_dict_with_new_column[metric_name]) for metric_name in sql_queries_dict_with_new_column}
final_sql_query_dict

