{% macro source_golden_records_hash_consistent(golden_record_model, hashed_model, join_column) %}


WITH sheetload_data AS (

    SELECT
        {{ hash_sensitive_columns(golden_record_model)}}
    FROM {{ ref(golden_record_model"_golden_record") }}

), hashed_data AS (

    SELECT *
    FROM {{ ref(hashed_model) }}

), check_data AS (

{% set meta_columns = get_meta_columns(golden_record_model, "sensitive") %}

    SELECT
        sheetload.{{ join_column }},
         SUM(
        {%- for column in meta_columns %}
        CASE WHEN
            IFNULL(sheetload.{{ column }}_hash, '') = IFNULL(hashed.{{ column }}_hash, '') THEN 0 ELSE 1
        END
            {%- if not loop.last %}
                +
            {% endif %}
        {% endfor %}
        )
        AS num_rows
    FROM sheetload_data sheetload
    LEFT JOIN hashed_data hashed ON hashed.{{ join_column }} = sheetload.{{ join_column }}
    GROUP BY sheetload.{{ join_column }}
)

    SELECT *
    FROM check_data
    WHERE num_rows > 0

{% endmacro %}
