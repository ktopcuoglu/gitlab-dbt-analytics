WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_design_management_designs_versions_dedupe_source') }}
    
), renamed AS (

    SELECT
      MD5(id)                                     AS design_version_id,
      design_id::VARCHAR                          AS design_id,
      version_id::NUMBER                         AS version_id,
      event::NUMBER                              AS event_type_id
    FROM source

)

SELECT *
FROM renamed
