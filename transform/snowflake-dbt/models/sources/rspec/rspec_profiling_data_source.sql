WITH source AS (

    SELECT *
    FROM {{ source('rspec', 'profiling_data') }}

), renamed AS (

    SELECT
      commit                   AS commit,
      commit_time              AS commit_time,
      total_time               AS total_time,
      number_of_tests          AS number_of_tests,
      time_per_single_test     AS time_per_single_test,
      total_queries            AS total_queries,
      total_query_time         AS total_query_time,
      total_requests           AS total_requests,
      _UPDATED_AT              AS updated_at
    FROM source

)

SELECT *
FROM renamed
