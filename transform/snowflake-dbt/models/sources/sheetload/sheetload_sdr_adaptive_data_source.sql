WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'sdr_adaptive_data') }}

), renamed as (

    SELECT
      current_month::DATE                   AS month,
      name::VARCHAR                         AS name,
      start_month::DATE                     AS start_month,
      add_internal_accounted::VARCHAR       AS add_internal_accounted,
      hiring_manager::VARCHAR               AS hiring_manager,
      role::VARCHAR                         AS role,
      region::VARCHAR                       AS region,
      segment::VARCHAR                      AS segment,
      ghpid::NUMBER                         AS GHPID,
      employment::VARCHAR                   AS employment,
      status::VARCHAR                       AS status
    FROM source
)

SELECT *
FROM renamed
