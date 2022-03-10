{%- macro scd_latest_state(table_name) -%}

WITH max_task_intance AS (
    SELECT MAX(_task_instance) AS _task_instance
    FROM {{table_name}}

), filtered AS (

    SELECT *
    FROM {{ table_name }}
    WHERE _task_instance = (

                            SELECT _task_instance
                            FROM max_task_instance

                            )

)

SELECT *
FROM filtered

{%- endmacro -%}
