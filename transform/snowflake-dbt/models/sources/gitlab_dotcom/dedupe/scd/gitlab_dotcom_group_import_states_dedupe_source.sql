WITH base AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'group_import_states') }}

)

{{ scd_latest_state(source='base', max_column='_task_instance') }}