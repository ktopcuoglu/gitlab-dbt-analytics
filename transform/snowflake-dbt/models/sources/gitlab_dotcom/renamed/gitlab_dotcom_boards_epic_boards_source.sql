WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_boards_epic_boards_dedupe_source') }}

), renamed AS (

    SELECT
      id::NUMBER                  as epic_board_id,
      hide_backlog_list::BOOLEAN  as hide_backlog_list,
      hide_closed_list::BOOLEAN   as hide_closed_list,
      group_id::NUMBER            as group_id,
      created_at::TIMESTAMP       as created_at,
      updated_at::TIMESTAMP       as updated_at
    FROM source

)

SELECT *
FROM renamed
