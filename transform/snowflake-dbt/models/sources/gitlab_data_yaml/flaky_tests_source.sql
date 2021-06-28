WITH source AS (

    SELECT *,
      RANK() OVER (PARTITION BY DATE_TRUNC('day', uploaded_at) ORDER BY uploaded_at DESC) AS rank
    FROM {{ source('gitlab_data_yaml', 'flaky_tests') }}
    ORDER BY uploaded_at DESC

), intermediate AS (

    SELECT d.value                          AS data_by_row,
    date_trunc('day', uploaded_at)::date    AS snapshot_date,
    rank
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), OUTER => TRUE) d

), renamed AS (

    SELECT
      data_by_row['hash']::VARCHAR             AS hash,
      data_by_row['example_id']::VARCHAR       AS example_id,
      data_by_row['file']::VARCHAR             AS file,
      data_by_row['line']::INT                 AS line,
      data_by_row['description']::VARCHAR      AS description,
      data_by_row['last_flaky_job']::VARCHAR   AS last_flaky_job,
      data_by_row['last_attempts_count']::INT  AS last_attempts_count,
      data_by_row['flaky_reports']::INT        AS flaky_reports,
      data_by_row['first_flaky_at']::TIMESTAMP AS first_flaky_at,
      data_by_row['last_flaky_at']::TIMESTAMP  AS last_flaky_at,
      snapshot_date,
      rank
    FROM intermediate

)

SELECT *
FROM renamed
 
