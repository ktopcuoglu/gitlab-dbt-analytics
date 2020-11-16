WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'usage_ping_metrics_sections') }}

), renamed AS (

    SELECT 
      section::VARCHAR                AS section_name,
      metrics_path::VARCHAR           AS metrics_path,
      stage::VARCHAR                  AS stage_name,
      "group"::VARCHAR                AS group_name,
      smau::BOOLEAN                   AS is_smau,
      gmau::BOOLEAN                   AS is_gmau,
      clean_metric_name::VARCHAR      AS clean_metrics_name,
      periscope_metrics_name::VARCHAR AS periscope_metrics_name,
      time_period::VARCHAR            AS time_period,
      mau::BOOLEAN                    AS is_umau,
      paid_gmau::BOOLEAN              AS is_paid_gmau
    FROM source 

)

SELECT *
FROM renamed
