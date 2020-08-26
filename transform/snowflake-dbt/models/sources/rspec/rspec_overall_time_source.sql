WITH source AS (

    SELECT *
    FROM {{ source('rspec', 'overall_time') }}

), renamed AS (

    SELECT
      commit::VARCHAR                       AS commit,
      commit_time::TIMESTAMP_TZ             AS commit_time_at,
      total_time::FLOAT                     AS total_time,
      number_of_tests::FLOAT                AS number_of_tests,
      time_per_single_test::FLOAT           AS time_per_single_test,
      total_queries::FLOAT                  AS total_queries,
      total_query_time::FLOAT               AS total_query_time,
      total_requests::FLOAT                 AS total_requests,
      _UPDATED_AT::FLOAT                    AS updated_at
    FROM source

)

SELECT *
FROM renamed
