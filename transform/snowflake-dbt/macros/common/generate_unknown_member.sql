{%- macro generate_unknown_member(model_name) -%}

    {%- set table_schema = dbt_utils.get_query_results_as_dict(
        "SELECT
           LOWER(SPLIT_PART(value, ' ', 1)) AS column_name,
           CASE
             WHEN value ILIKE 'DIM_% VARCHAR%' OR value ILIKE '%_ID VARCHAR%'
                THEN 'CHAR_KEY'
             WHEN value ILIKE '%_ID NUMBER%'
               THEN 'NUM_KEY'
               ELSE TRIM(SPLIT_PART(SPLIT_PART(SPLIT_PART(value, ' ', 2), '_', 1), '(', 1), ',')
           END                              AS column_type
            FROM LATERAL SPLIT_TO_TABLE(GET_DDL('table', '" ~ ref(model_name) ~ "'), '\n' || char(9))
            WHERE index > 1"
    ) %}

    UNION ALL
    
    SELECT 
    {% for column in table_schema.COLUMN_NAME %}
    {%- if table_schema.COLUMN_TYPE[loop.index0] == 'CHAR_KEY' %}
      MD5('-1')                             AS {{ column }}
    {%- elif table_schema.COLUMN_TYPE[loop.index0] == 'NUM_KEY' %}
      -1                                    AS {{ column }}
    {%- elif table_schema.COLUMN_TYPE[loop.index0] == 'VARCHAR' %}
      'Unknown {{ column }}'                AS {{ column }}
    {%- elif table_schema.COLUMN_TYPE [loop.index0] == 'NUMBER' %}
      0                                     AS {{ column }}
    {%- elif table_schema.COLUMN_TYPE[loop.index0] == 'FLOAT' %}
      0.0                                   AS {{ column }}
    {%- elif table_schema.COLUMN_TYPE[loop.index0] == 'TIMESTAMP' %}
      '9999-12-31 00:00:00.000 +0000'       AS {{ column }}
    {%- elif table_schema.COLUMN_TYPE[loop.index0] == 'DATE' %}
      '9999-12-31'                          AS {{ column }}
    {%- elif table_schema.COLUMN_TYPE[loop.index0] == 'BOOLEAN' %}
      FALSE                                 AS {{ column }}
    {%- else %}
      NULL                                  AS {{ column }}
    {%- endif -%}
    {%- if not loop.last %},{% endif -%}
    {% endfor %}

{%- endmacro -%}
