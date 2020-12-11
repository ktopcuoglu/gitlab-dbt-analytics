WITH source AS (

    SELECT *,
      RANK() OVER (PARTITION BY DATE_TRUNC('day', uploaded_at) ORDER BY uploaded_at DESC) AS rank
    FROM {{ source('gitlab_data_yaml', 'feature_flags') }}
    ORDER BY uploaded_at DESC

), intermediate AS (

    SELECT d.value                          AS data_by_row,
    date_trunc('day', uploaded_at)::date    AS snapshot_date,
    rank
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), OUTER => TRUE) d

), renamed AS (

    SELECT
      data_by_row['name']::VARCHAR            AS name,
      data_by_row['type']::VARCHAR            AS type,
      data_by_row['milestone']::VARCHAR       AS milestone,
      data_by_row['default_enabled']::BOOLEAN AS default_enabled,
      data_by_row['group']::VARCHAR           AS gitlab_group,
      snapshot_date,
      rank
    FROM intermediate

)

SELECT *
FROM renamed
 