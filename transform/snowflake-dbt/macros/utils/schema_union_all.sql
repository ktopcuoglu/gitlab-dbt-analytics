{%- macro schema_union_all(schema_part, table_name, exclude_part='scratch', database_name=none, day_limit=none) -%}

 {%- if database_name is not none -%}

    {%- set database = database_name -%}

 {%- else -%}

    {%- set database = target.database -%}

 {%- endif -%}

 {%- call statement('get_schemata', fetch_result=True) -%}

  SELECT DISTINCT 
    '"' || table_schema || '"."' || table_name || '"' AS qualified_name,
    table_schema,
    table_name,
    CASE
      WHEN REGEXP_INSTR(table_name,'\\d{4}_\\d{2}_\\d{2}') > 0 THEN 'YYYY_MM_DD'
      WHEN REGEXP_INSTR(table_name,'\\d{4}_\\d{2}') > 0 THEN 'YYYY_MM'
    END AS table_date_format,
    CASE
      WHEN REGEXP_INSTR(table_schema,'\\d{4}_\\d{2}_\\d{2}') > 0 THEN 'YYYY_MM_DD'
      WHEN REGEXP_INSTR(table_schema,'\\d{4}_\\d{2}') > 0 THEN 'YYYY_MM'
    END AS schema_date_format,
    TO_DATE(REGEXP_SUBSTR(table_name,'\\d{4}_\\d{2}_?\\d{0,2}'),table_date_format) as table_date,
    TO_DATE(REGEXP_SUBSTR(table_schema,'\\d{4}_\\d{2}_?\\d{0,2}'),schema_date_format) as schema_date
  FROM "{{ database }}".information_schema.tables
  WHERE table_schema ILIKE '%{{ schema_part }}%'
    AND table_schema NOT ILIKE '%{{ exclude_part }}%'
    AND table_name ILIKE '{{ table_name }}'
    {%- if day_limit %}
    AND COALESCE(table_date,schema_date) >= DATE_TRUNC('month',DATEADD('day',-{{ day_limit }}, CURRENT_DATE()))
    {%- endif -%}
  ORDER BY 1

  {%- endcall -%}

    {%- set value_list = load_result('get_schemata') -%}

    {%- if value_list and value_list['data'] -%}

        {%- set values = value_list['data'] | map(attribute=0) | list -%}
            
          {% for schematable in values -%}
                SELECT *
                FROM "{{ database }}".{{ schematable }}
                {% if not loop.last %}
                UNION ALL
                {% endif -%}
              {% endfor %}
            
    {%- else -%}

        {{ return(1) }}

    {%- endif %}


{%- endmacro -%}
