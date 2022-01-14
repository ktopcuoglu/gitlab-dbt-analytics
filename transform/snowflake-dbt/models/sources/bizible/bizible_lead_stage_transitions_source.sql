WITH source AS (

    SELECT
      id                        AS id,
      email                     AS email,
      lead_id                   AS lead_id,
      contact_id                AS contact_id,
      touchpoint_id             AS touchpoint_id,
      transition_date           AS transition_date,
      stage_id                  AS stage_id,
      stage                     AS stage,
      rank                      AS rank,
      index                     AS index,
      last_index                AS last_index,
      is_pending                AS is_pending,
      is_non_transitional       AS is_non_transitional,
      previous_stage_date       AS previous_stage_date,
      next_stage_date           AS next_stage_date,
      modified_date             AS modified_date,
      is_deleted                AS is_deleted,
      _created_date             AS created_date,
      _modified_date            AS modified_date,
      _deleted_date             AS deleted_date
    FROM {{ source('bizible', 'biz_lead_stage_transitions') }}
    ORDER BY uploaded_at DESC

)

SELECT *
FROM source

