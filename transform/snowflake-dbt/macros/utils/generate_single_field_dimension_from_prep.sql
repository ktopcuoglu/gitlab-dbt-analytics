{% macro generate_single_field_dimension_from_prep(model_name, dimension_column) %}

-- Safer / less confusing to break up these replaces
-- Setup dimension column
{% set dimension_column_name = dimension_column|replace('_source', '')  %}
{% set dimension_column_name = dimension_column|replace('dim_', '')  %}

-- Setup id column
{% set id_column_name = id_column_name|replace('_source', '')  %}
{% set id_column_name = id_column_name|replace('_name', '')  %}
{% set id_column_name = id_column_name ~ '_id'  %}
{% set id_column_name = id_column_name|replace('', '')  %}

WITH source_data AS (

    SELECT {{ dimension_column }}
    FROM {{ ref(model_name) }}
    WHERE {{ dimension_column }} IS NOT NULL

), unioned AS (

    SELECT DISTINCT
      {{ dbt_utils.surrogate_key([dimension_column]) }}     AS {{ id_column_name }},
      {{  dimension_column }}                               AS {{ dimension_column_name }}
    FROM source_data
    UNION ALL
    SELECT
      MD5('-1')                                     AS {{ id_column_name }},
      'Missing {{dimension_column_name}}'           AS {{ dimension_column_name }}

)

{%- endmacro -%}
