{% macro generate_map_area(model_name, id_column, id_column_name, dimension_column, dimension_column_name) %}

{% set dedup_column_name = 'dimension_column_name_dedup' %}

WITH source_data AS (

    SELECT *
    FROM {{ ref(model_name) }}
    WHERE {{ dimension_column }} IS NOT NULL

), map_data AS (

    SELECT DISTINCT
          {{  dimension_column }}                               AS {{  dimension_column }},
          UPPER(TRIM({{  dimension_column }}))                  AS {{   dedup_column_name }},
          {{ dbt_utils.surrogate_key([dedup_column_name]) }}    AS {{ id_column_name }},
          MAX({{  dimension_column }}) OVER (Partition by {{  id_column_name }}) AS {{ dimension_column_name }}
    FROM source_data
)
{%- endmacro -%}
