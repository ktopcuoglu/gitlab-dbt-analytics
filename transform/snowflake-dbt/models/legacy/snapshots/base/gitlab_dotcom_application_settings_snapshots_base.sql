{{ config({
    "alias": "gitlab_dotcom_application_settings_snapshots"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('snapshots', 'gitlab_dotcom_application_settings_snapshots') }}
    
), renamed as (

  SELECT
  
    dbt_scd_id::VARCHAR                                           AS application_settings_snapshot_id,
    id::NUMBER                                                    AS application_settings_id,
    shared_runners_minutes::NUMBER                                AS shared_runners_minutes

  FROM source
    
)

SELECT *
FROM renamed
