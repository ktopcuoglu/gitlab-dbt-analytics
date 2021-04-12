WITH source AS (

    SELECT *
    FROM {{ source('gitlab_data_yaml', 'usage_ping_metrics') }}

), intermediate AS (

    SELECT
      d.value                                 AS data_by_row,
      date_trunc('day', uploaded_at)::date    AS snapshot_date
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), OUTER => TRUE) d

), renamed AS (

     SELECT 
      data_by_row['key_path']         AS metrics_path,
      data_by_row['product_category'] AS product_category,
      data_by_row['product_group']    AS product_group,
      data_by_row['product_section']  AS product_section,
      data_by_row['product_stage']    AS product_stage,
      data_by_row['skip_validation']  AS skip_validation,
      data_by_row['status']           AS metrics_status,
      data_by_row['tier']             AS tier,
      data_by_row['time_frame']       AS time_frame,
      data_by_row['value_type']       AS value_type,
      snapshot_date
    FROM intermediate

), intermediate_stage AS (

    SELECT 
      renamed.*
    FROM renamed

), final AS (

    SELECT *,
      FIRST_VALUE(snapshot_date) OVER (PARTITION BY metrics_path ORDER BY snapshot_date) AS date_first_added, 
      MIN(snapshot_date) OVER (PARTITION BY unique_key ORDER BY snapshot_date)      AS valid_from_date,
      MAX(snapshot_date) OVER (PARTITION BY unique_key ORDER BY snapshot_date DESC) AS valid_to_date
    FROM intermediate_stage

)

SELECT *
FROM final
