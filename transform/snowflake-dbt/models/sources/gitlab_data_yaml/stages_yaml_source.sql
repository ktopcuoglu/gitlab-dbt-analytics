WITH source AS (

    SELECT *,
      RANK() OVER (PARTITION BY DATE_TRUNC('day', uploaded_at) ORDER BY uploaded_at DESC) AS rank
    FROM {{ source('gitlab_data_yaml', 'stages') }}

), intermediate AS (

    SELECT
      d.value                                 AS data_by_row,
      date_trunc('day', uploaded_at)::date    AS snapshot_date,
      rank
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext['stages']), OUTER => TRUE) d

), intermediate_stage AS (

    SELECT
      data_by_row['pm']::VARCHAR                             AS project_manager,
      data_by_row['display_name']::VARCHAR                   AS stage_display_name,
      data_by_row['established']::NUMBER                     AS year_established,
      data_by_row['lifecycle']::NUMBER                       AS lifecycle,
      data_by_row['horizon']::VARCHAR                        AS horizon,
      data_by_row['contributions']::NUMBER                   AS contributions,
      data_by_row['usage_driver_score']::NUMBER              AS usage_driver_score,
      data_by_row['sam_driver_score']::NUMBER                AS sam_driver_score,
      data_by_row['related']::ARRAY                          AS related_stages,
      data_by_row['stage_development_spend_percent']::NUMBER AS stage_development_spend_percent,
      data_by_row['groups']::ARRAY                            AS stage_groups
    FROM intermediate
      
)

SELECT *
FROM intermediate_stage
