{{ config({
    "materialized": "incremental",
    "unique_key": "metric_id"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ci_platform_metrics_dedupe_source') }}

    {% if is_incremental() %}

      WHERE recorded_at >= (SELECT MAX(recorded_at) FROM {{this}})

    {% endif %}

    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY recorded_at DESC) = 1

), renamed AS (

    SELECT
      id::NUMBER                                      AS metric_id,
      recorded_at::TIMESTAMP                          AS recorded_at,
      platform_target::VARCHAR                        AS platform_target,
      count::NUMBER                                   AS target_count                     
    FROM source

)

SELECT *
FROM renamed
