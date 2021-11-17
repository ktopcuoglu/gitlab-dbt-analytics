WITH source as (

    SELECT *
    FROM {{ source('greenhouse', 'interviewers') }}
    -- intentionally excluded record, details in https://gitlab.com/gitlab-data/analytics/-/issues/10211
    WHERE NOT (user_id      = 4932640002
    AND interview_id        = 101187575002
    AND scorecard_id        IS NULL
    AND user                IS NULL)

), renamed as (

  SELECT distinct user AS interviewer_name
  FROM source

)

SELECT *
FROM renamed
