WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'sdr_adaptive_data') }}

), renamed as (

    SELECT
      "Current Month"::DATE                                 AS month,
      "Name"::VARCHAR                                       AS name,
      "Start Month"::DATE                                   AS start_month,
      "Add/Internal/Accounted for Currently"::VARCHAR       AS add_internal_accounted,
      "Hiring Manager"::VARCHAR                             AS hiring_manager,
      "Role"::VARCHAR                                       AS role,
      "Region"::VARCHAR                                     AS region,
      "Segment"::VARCHAR                                    AS segment,
      "GHPID"::NUMBER                                       AS GHPID,
      "Employment"::VARCHAR                                  AS employment,
      "Status"::VARCHAR                                     AS status
    FROM source
)

SELECT *
FROM renamed
