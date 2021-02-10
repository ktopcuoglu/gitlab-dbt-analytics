WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_project_import_data_dedupe_source') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1

), renamed AS (

    SELECT

      id::NUMBER                      AS project_import_relation_id,
      project_id::NUMBER              AS project_id

    FROM source

)

SELECT *
FROM renamed
